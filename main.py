import time
import arxivpy


machine_learning_categories = ['cs.CV', 'cs.CL', 'cs.LG', 'cs.AI', 'cs.NE', 'stat.ML']
article_list = []

start_index = 0
STEP = 100
articles_per_minute = STEP * 2

article_len = articles_per_minute

while article_len == articles_per_minute:
    # query 100 results per iteration
    # wait 30 seconds per query
    articles = arxivpy.query(search_query=machine_learning_categories,
                             start_index=start_index, max_index=articles_per_minute, results_per_iteration=STEP,
                             wait_time=30, sort_by='lastUpdatedDate', sort_order='ascending')

    # crawling log
    print('last: ' + articles[-1]['published'])
    print(str(start_index + STEP * 2) + ' articles crawled')

    # compute article_len
    article_len = len(articles)
    article_list.append(articles)

    # compute start_index
    start_index += STEP * 2

    # sleep 1 minute
    time.sleep(60)
