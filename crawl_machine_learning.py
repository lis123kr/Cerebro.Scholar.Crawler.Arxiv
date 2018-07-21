import time
import arxivpy
import logging
from article import Article
from connect_database import connect_database


# start_index = -1 : auto option
def crawl_machine_learning(start_index: int, sort_order: str):
    conn = connect_database()

    machine_learning_categories = ['cs.CV', 'cs.CL', 'cs.LG', 'cs.AI', 'cs.NE', 'stat.ML']

    STEP = 100
    articles_per_minute = STEP * 2

    article_len = articles_per_minute

    if start_index == -1:
        start_index = Article.get_n_articles(conn) - STEP

    logging.info('crawling start')
    logging.info('start index : ' + str(start_index))
    logging.info('sort_order : ' + sort_order)

    while article_len == articles_per_minute:
        # query 100 results per iteration
        # wait 30 seconds per query
        try:
            articles = arxivpy.query(search_query=machine_learning_categories,
                                     start_index=start_index, max_index=start_index + articles_per_minute,
                                     results_per_iteration=STEP,
                                     wait_time=30, sort_by='lastUpdatedDate', sort_order=sort_order)

            # crawling log
            logging.info('last: ' + articles[-1]['published'])
            logging.info(str(start_index + STEP * 2) + ' articles crawled')

            # save articles
            for article in articles:
                Article(article, conn).save()

            # compute start_index
            start_index += STEP * 2

            # compute article_len
            article_len = len(articles)

            # sleep 1 minute
            time.sleep(60)
        except Exception as e:
            logging.error(e)
            time.sleep(60 * 30)

    conn.close()
