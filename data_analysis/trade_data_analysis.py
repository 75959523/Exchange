import os

from psycopg2.extras import DictCursor
from config.logger_config import setup_logger
from database.db_pool import get_connection, release_connection
from config.factor_config import MIN_PERCENTAGE
from config.factor_config import MAX_PERCENTAGE

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = setup_logger("fetch_combined_analysis_data", os.path.join(project_root, 'log', 'app.log'))


def fetch_combined_analysis_data(temp_table):
    try:
        connection = get_connection()
        cursor = connection.cursor(cursor_factory=DictCursor)

        sql_script = f"""
            WITH AdjustedPrices AS (
                SELECT
                    t.*,
                    s.price
                FROM {temp_table} t
                LEFT JOIN stablecoin s ON t.reference = s.symbol_name AND t.exchange_name = s.exchange_name
                WHERE bid != 0
                AND ask != 0
                AND price != 0
                AND status = 1
            ),
            Combined AS (
                SELECT
                    b.symbol_name,
                    b.bid max_bid,
                    a.ask min_ask,
                    b.exchange_name max_bid_exchange,
                    a.exchange_name min_ask_exchange,
                    b.reference bid_reference,
                    a.reference ask_reference,
                    b.price bid_reference_price,
                    a.price ask_reference_price,
                    ROUND((((b.bid * b.price - a.ask * a.price) / (a.ask * a.price)) * 100), 2) price_diff_percentage
                FROM AdjustedPrices b, AdjustedPrices a
                WHERE b.symbol_name = a.symbol_name
                AND b.exchange_name <> a.exchange_name
                AND (b.bid * b.price) > (a.ask * a.price)
            ),
            FilteredRecords AS (
                SELECT *
                FROM Combined
                WHERE price_diff_percentage BETWEEN {MIN_PERCENTAGE} AND {MAX_PERCENTAGE}
            ),
            CombinedChain AS (
                SELECT * FROM chain
                UNION ALL
                SELECT * FROM chain_custom
            ),
            ExchangeChainsJoined AS (
                SELECT
                    f.*,
                    ask_ec.chain ask_chain,
                    bid_ec.chain bid_chain,
                    ask_ec.canwd ask_canwd,
                    bid_ec.candep bid_candep,
                    ask_ec.ct_addr ask_ct_addr,
                    bid_ec.ct_addr bid_ct_addr
                FROM FilteredRecords f
                LEFT JOIN CombinedChain ask_ec ON f.min_ask_exchange = ask_ec.exchange_name AND f.symbol_name = ask_ec.ccy
                LEFT JOIN CombinedChain bid_ec ON f.max_bid_exchange = bid_ec.exchange_name AND f.symbol_name = bid_ec.ccy
            ),
            RankedRecords AS (
                SELECT
                    e.symbol_name,
                    e.price_diff_percentage,
                    e.min_ask_exchange,
                    e.max_bid_exchange,
                    e.min_ask,
                    e.ask_reference,
                    e.max_bid,
                    e.bid_reference,
                    0 ask_size,
                    0 bid_size,
                    0 total_ask_amount_cny,
                    0 total_bid_amount_cny,
                    CASE
                        WHEN 
                            ((e.ask_chain ILIKE '%' || e.bid_chain || '%' OR e.bid_chain ILIKE '%' || e.ask_chain || '%')
                            AND e.ask_canwd = 'true'
                            AND e.bid_candep = 'true')
                        THEN e.ask_chain
                        WHEN e.ask_chain IS NULL THEN 'Ask Missing'
                        WHEN e.bid_chain IS NULL THEN 'Bid Missing'
                    END chain_status,
                    CASE
                        WHEN
                            TRIM(e.ask_ct_addr) = '' AND TRIM(e.bid_ct_addr) = '' 
                        THEN 'Both Empty'
                        WHEN
                            (e.ask_ct_addr ILIKE '%' || e.bid_ct_addr OR e.bid_ct_addr ILIKE '%' || e.ask_ct_addr)
                            AND
                            (UPPER(SUBSTRING(e.ask_ct_addr FROM LENGTH(e.ask_ct_addr) - 5)) = UPPER(SUBSTRING(e.bid_ct_addr FROM LENGTH(e.bid_ct_addr) - 5)))
                        THEN 'Verified'
                        ELSE
                            CONCAT(
                                SUBSTRING(e.ask_ct_addr FROM LENGTH(e.ask_ct_addr) - 5), ',',
                                SUBSTRING(e.bid_ct_addr FROM LENGTH(e.bid_ct_addr) - 5)
                            )
                    END ct_status,
                    ROW_NUMBER() OVER(
                        PARTITION BY symbol_name, price_diff_percentage, min_ask_exchange, max_bid_exchange, ask_reference, bid_reference
                        ORDER BY
                            CASE
                                WHEN
                                    TRIM(e.ask_ct_addr) != '' AND TRIM(e.bid_ct_addr) != ''
                                    AND
                                    (e.ask_ct_addr ILIKE '%' || e.bid_ct_addr OR e.bid_ct_addr ILIKE '%' || e.ask_ct_addr)
                                    AND
                                    (UPPER(SUBSTRING(e.ask_ct_addr FROM LENGTH(e.ask_ct_addr) - 5)) = UPPER(SUBSTRING(e.bid_ct_addr FROM LENGTH(e.bid_ct_addr) - 5)))
                                THEN 0
                                ELSE 1
                            END
                    ) rn_chain
                FROM ExchangeChainsJoined e
                    WHERE
                        ((e.ask_chain ILIKE '%' || e.bid_chain || '%' OR e.bid_chain ILIKE '%' || e.ask_chain || '%')
                        AND e.ask_canwd = 'true'
                        AND e.bid_candep = 'true')
                        OR e.ask_chain IS NULL
                        OR e.bid_chain IS NULL
                ORDER BY e.price_diff_percentage DESC
            )
            INSERT INTO {temp_table}_analysis
                SELECT * FROM RankedRecords
                WHERE rn_chain = 1
    """
        cursor.execute(sql_script)
        connection.commit()
        cursor.close()
        release_connection(connection)
    except Exception as e:
        logger.error(f"Error occurred while fetch_combined_analysis_data Error: {repr(e)}")
