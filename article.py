import re


class Article:
    def __init__(self, entry: dict):
        self.title = entry['title_detail']['value']
        self.abstract = entry['summary_detail']['value']
        self.doi = entry.get('arxiv_doi')
        self.arXiv_id, self.version = Article.get_arxiv_id(entry['id'])
        self.authors = [{'fullname' : author['name']} for author in entry['authors']]
        self.start_date = entry['published']
        self.pub_year = self.start_date[0:4]
        self.publication = entry.get('arxiv_journal_ref')

    @classmethod
    def get_arxiv_id(cls, arxiv_id):
        arxiv_id = arxiv_id.replace("http://arxiv.org/abs/", "")
        version = None
        if re.compile('v[1-9]').match(arxiv_id[-2:]):
            arxiv_id = arxiv_id[:-2]
            version = arxiv_id[-2:]
        return arxiv_id, version
