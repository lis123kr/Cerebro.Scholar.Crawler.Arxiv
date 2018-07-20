import logging
import re
import json


class Article:
    def __init__(self, entry: dict, conn):
        self.title = entry['title_detail']['value']
        self.abstract = entry['summary_detail']['value']
        self.doi = entry.get('arxiv_doi')
        self.arXiv_id, self.version = Article.get_arxiv_id(entry['id'])
        self.authors = [{'fullname' : author['name']} for author in entry['authors']]
        self.start_date = entry['published']
        self.pub_year = self.start_date[0:4]
        self.publication = entry.get('arxiv_journal_ref')
        self.conn = conn

    def is_saved(self):
        return self.find() is not None

    def save(self):
        find = self.find()
        if find is None:
            sql_base = 'INSERT INTO articles'
            sql_elements = ['title', 'abstract', 'doi', 'arXiv_id', 'authors', 'start_date', 'pub_year', 'publication']
            logging.info(self.arXiv_id)
            items = (
                        self.title,
                        self.abstract,
                        self.doi,
                        self.arXiv_id,
                        self.authors.__str__(),
                        self.start_date,
                        self.pub_year,
                        self.publication
                    )
            if self.version is not None:
                sql_elements.append('version')
                items = items + (json.dumps([self.version]),)
            sql = sql_base + ' (' + ', '.join(sql_elements) + ') ' + 'VALUES' + ' (' + ', '.join(['%s'] * len(sql_elements)) + ')'
            with self.conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    items
                )
                self.conn.commit()
        else:
            version_list = self.get_version_list()
            if self.version not in version_list:
                self.add_version()

    def find(self):
        with self.conn.cursor() as cursor:
            sql = 'SELECT * FROM articles WHERE arXiv_id = %s'
            cursor.execute(sql, self.arXiv_id)
            result = cursor.fetchone()
            return result

    def get_version_list(self):
        result = self.find()

        version_list = json.loads(result['version'])
        return version_list

    def add_version(self):
        version_list = self.get_version_list()
        version_list.append(self.version)

        with self.conn.cursor() as cursor:
            sql = 'UPDATE articles SET version = %s WHERE arXiv_id = %s'
            cursor.execute(sql, (json.dumps(version_list), self.arXiv_id))

    @classmethod
    def get_arxiv_id(cls, arxiv_id):
        arxiv_id = arxiv_id.replace("http://arxiv.org/abs/", "")
        version = None
        if re.compile('v[1-9]').match(arxiv_id[-2:]):
            version = arxiv_id[-1]
            arxiv_id = arxiv_id[:-2]
        return arxiv_id, version

    @classmethod
    def get_n_articles(cls, conn):
        with conn.cursor() as cursor:
            sql = 'SELECT COUNT(arXiv_id) FROM Articles'
            return cursor.execute(sql)
