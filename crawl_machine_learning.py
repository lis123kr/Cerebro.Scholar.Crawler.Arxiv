import time
import arxivpy
import logging
from article import Article
from connect_database import connect_database


def crawl_machine_learning():
    conn = connect_database()
    machine_learning_categories = ['cs.CV', 'cs.CL', 'cs.LG', 'cs.AI', 'cs.NE', 'stat.ML']

    start_index = 0
    STEP = 100
    articles_per_minute = STEP * 2

    article_len = articles_per_minute

    while article_len == articles_per_minute:
        # query 100 results per iteration
        # wait 30 seconds per query
        articles = arxivpy.query(search_query=machine_learning_categories,
                                 start_index=start_index, max_index=start_index + articles_per_minute,
                                 results_per_iteration=STEP,
                                 wait_time=30, sort_by='lastUpdatedDate', sort_order='ascending')

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

    conn.close()
