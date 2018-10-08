import time
import arxivpy
import logging
from article import Article
from AWS_utils import RDS_utils, Duplication_check
from qtitle import Qtitle
import ast
# from connect_database import connect_database
MINUTE = 60
Memory_Eff = 200000

def arxiv_id_check(DBU):
    axvlist = DBU.DB.select("select arxiv_id, p_id from paper where arxiv_id != 'None'", ())
    if len(axvlist) <= Memory_Eff:
        axvdict = dict()
        for x in axvlist:
            axvdict[x[0]] = x[1]
        return axvdict
    return None

def get_qtitle(line):
    qt = None
    Special_Char = Qtitle.get_Special_Char()
    if line.strip() != '':
        qt = line.strip()
        for i,j in zip(Special_Char['full'], Special_Char['half']):
            line = line.replace(i, j)
            qt = qt.replace(i, '').replace(j, '')
        return line, qt.upper()
    else:
        return line, qt

def Update_aXv_paper(ori, data):
    # title
    title = ast.literal_eval(str(ori[1]))
    if not title.get('axv'):
        title['axv'] = data[1]
        ori[1] = str(title)

    #abstract
    if str(ori[2]) == 'None':
        ori[2] = data[2]

    # version
    version = ast.literal_eval(str(ori[3]))
    if not version.get('axv'):
        version['axv'] = data[3]
        ori[3] = str(version)

    # authors
    authors = ast.literal_eval(str(ori[4]))
    if not authors.get('axv'):
        authors['axv'] = data[4]
        ori[4] = str(authors)

    # start_date
    start_date = ast.literal_eval(str(ori[5]))
    if not start_date.get('axv'):
        start_date['axv'] = data[5]
        ori[5] = str(start_date)

    # pub_year
    pub_year = ast.literal_eval(str(ori[6]))
    if not pub_year.get('axv'):
        pub_year['axv'] = data[6]
        ori[6] = str(pub_year)

    # publication
    publication = ast.literal_eval(str(ori[7]))
    if not publication.get('axv'):
        publication['axv'] = data[7]
        ori[7] = str(publication)

    return ori

# start_index = -1 : auto option
def crawl_machine_learning(start_index: int, sort_order: str):
    # conn = connect_database()
    
    DBU = RDS_utils()

    check_axv = arxiv_id_check(DBU) 

    machine_learning_categories = ['cs.CV', 'cs.CL', 'cs.LG', 'cs.AI', 'cs.NE', 'stat.ML', 'cs.MA']

    STEP = 100
    articles_per_minute = STEP * 2

    article_len = articles_per_minute

    # if start_index == -1:
    #     start_index = Article.get_n_articles(conn) - STEP

    logging.info('crawling start')
    logging.info('start index : ' + str(start_index))
    logging.info('sort_order : ' + sort_order)

    update_paper, insert_paper, insertfail, updatafail = 0, 0, 0,0
    while article_len == articles_per_minute:
        # query 100 results per iteration
        # wait 30 seconds per query
        try:
            start = time.time()
            articles = arxivpy.query(search_query=machine_learning_categories,
                                     start_index=start_index, max_index=start_index + articles_per_minute,
                                     results_per_iteration=STEP,
                                     wait_time=5, sort_by='lastUpdatedDate', sort_order=sort_order)
            # crawling log
            # logging.info('last: ' + articles[-1].get('published', ''))
            logging.info(str(start_index + STEP * 2) + ' articles crawled')

            # save articles
            for article in articles:
                data = Article(article, None).tolist()

                print("'{}' cralwed / arxiv_id : {}".format(data[1], data[0]))
                axvid, pubyear = data[0], data[6]
                data[1], qt = get_qtitle(data[1]) # title      
                if pubyear:
                    qt = qt.strip()+str(pubyear)
                pid = None
                if type(check_axv) == dict:
                    if check_axv.get(axvid):
                        pid = check_axv[axvid]
                else:                    
                    pid = DBU.get_pid_from_arXiv_id(axvid)
                if not pid:
                    pid = Duplication_check.check(qt)

                if pid:
                    ori = DBU.get_paper_by_p_id(pid)
                    data = Update_aXv_paper(ori, data)
                    if DBU.update_axv(pid, data):
                        update_paper +=1
                    else:
                        updatafail+=1
                else:                    
                    if DBU.insert_axv(data):
                        pid = DBU.get_pid_from_arXiv_id(axvid)
                        Duplication_check.insert_title_year(qt, pid)
                        insert_paper +=1
                    else:
                        insertfail +=1

            # compute start_index
            start_index += STEP * 2

            # compute article_len
            article_len = len(articles)
            e = int(time.tim() - start)
            print('took {:02d}:{:02d}:{:02d} to crawl {} paper'.format(e // 3600, (e % 3600 // 60), e % 60, article_len))

            # sleep 1 minute, no 30 seconds
            time.sleep(MINUTE / 2)
        except Exception as e:
            logging.error(e)
            print("insert fail : {}, update fail : {}".format(insertfail, updatafail))
            DBU.DB.conn.close()
            return start_index, insert_paper, update_paper

    print("insert fail : {}, update fail : {}".format(insertfail, updatafail))
    DBU.DB.conn.close()
    return start_index, insert_paper, update_paper
    
