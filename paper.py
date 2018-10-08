# import logging
import re, json
from ast import literal_eval

def get_data(dict_, key):
    if dict_ == None or type(dict_) != dict:
        return None
    return dict_[key] if dict_.get(key) else None

class Qtitle:
    Special_Char = None
    @classmethod
    def get_Special_Char(cls):
        if cls.Special_Char == None:
            import pickle as pk
            cls.Special_Char = pk.load(open("SpecialChar.pkl", "rb"))
        return cls.Special_Char

class Paper:
    def __init__(self, doc, src, cite_text=None):
        paper_ = get_data(doc.data, 'coredata')
        if not paper_:
            print('[', doc.int_id, '] No Coredata')
            return None
        bibrecord = get_data(get_data(doc.data, 'item'), 'bibrecord')
        refs = get_data(get_data( bibrecord, 'tail'), 'bibliography')
        self.doc = doc
        self.scp_id = doc.int_id
        self.abstract = get_data(get_data( bibrecord, 'head'), 'abstracts')       
        self.title = get_data(paper_, 'dc:title')
        self.start_date = get_data(paper_, 'prism:coverDate')
        self.aggType = get_data(paper_, 'prism:aggregationType')
        self.subType = get_data(paper_, 'subtypeDescription')
        self.pii = get_data(paper_, 'pii')
        self.eid = get_data(paper_, 'eid')
        self.doi = get_data(paper_, 'prism:doi')
        self.ISSN = get_data(paper_, 'prism:issn')
        self.bibText = cite_text,
        self.n_cite = get_data(paper_, 'citedby-count')
        self.n_ref = get_data(refs, '@refcount')
        self.publisher = get_data(paper_, 'dc:publisher')
        self.publication = get_data(paper_, 'prism:publicationName')
        self.vol = get_data(paper_, 'prism:volume')
        self.issue = get_data(paper_, 'prism:issueIdentifier')
        self.pages = get_data(paper_, 'prism:pageRange')

        # Affiliations
        self.affiliation = self.SCP_get_affiliations_list()
         # Authors
        self.authors = self.SCP_get_authors_list()
        # Author keywords    
        self.keywords = self.SCP_get_keywords_list()

        # references
        self.refs = get_data(refs, 'reference')
        # self.references = self.SCP_get_references(refs)

        self.pub_year = None
        # citation     
        if self.start_date:
            p = re.compile('[0-9]{4}').search(self.start_date)
            self.pub_year = int(p.string[p.start():p.end()]) if p else None
            # print(self.pub_year, self.start_date)
        self.references = None
        self.citations = None
        self.arXiv_id = self.extract_arxivId()

        self.IEEE_id = None
        self.uid = None
        self.version = None
        self.end_date = None

        self.headings = None
        self.subheadings = None
        self.subjects = self.SCP_get_subjects()
        self.src = src
        self.columns = ['p_id', 'doi','arXiv_id', 'IEEE_id', 'uid', 'scp_id', 'scp_eid', 'ISSN', 
             'title', 'authors','abstract','keywords','pub_year', 'version', 'start_date','end_date',
             'subType', 'aggType','bibText','n_cite','n_ref','publisher','publication', 'vol', 'issue', 
            'pages', 'affiliation','headings', 'subheadings', 'subjects', 'refers',  'citations']
        if self.title:
            self.title, self.qtitle = self.full_to_half(self.title)
            if self.pub_year:
                self.qtitle = self.qtitle + str(self.pub_year)
        else:
            self.qtitle = None

    @classmethod
    def get_columns(cls):
        return ['p_id', 'doi','arXiv_id', 'IEEE_id', 'uid', 'scp_id', 'scp_eid', 'ISSN', 
             'title', 'authors','abstract','keywords','pub_year', 'version', 'start_date','end_date',
             'subType', 'aggType','bibText','n_cite','n_ref','publisher','publication', 'vol', 'issue', 
            'pages', 'affiliation','headings', 'subheadings', 'subjects', 'refers',  'citations']

    def extract_arxivId(self):        
        if self.publication and self.publication.upper().find("ARXIV") != -1:
            aid = None
            r = [
                {'reg' : re.compile(r'ARXIV ?\w*?[,:]? ?([0-9]{4}[.,:\-]? ?[0-9]{3,5})'), 'gnum':1},
                {'reg' : re.compile(r'HTTP[S]://ARXIV.ORG(/\w*)?(/\w*)?/([0-9]{3,4}[.,:\- ][0-9]{3,5})'), 'gnum':3},
                {'reg' : re.compile(r'(([0-9]{4}:)?([0-9]{3,4}[., :\-] ?[0-9]{3,5}))'), 'gnum':3}
            ]
            for rr in r:
                match = rr['reg'].search(self.publication.upper())
                if match:
                    aid = match.group(rr['gnum'])
                    break                
            if aid:
                aid = aid.replace(' ','')
                if aid[4] != '.':
                    aid = aid[:4] + '.' + aid[4:]
                return aid
        return None
                
    def SCP_get_affiliations_list(self):
        def SCP_get_aff(a_):
            aff = { 'aff_id' : get_data(a_, '@id'), 
                    'name' : get_data(a_, 'affilname'),
                    'city' : get_data(a_, 'affiliation-city'),
                    'country' : get_data(a_, 'affiliation-country')
                  }
            return aff
        affs = get_data(self.doc.data, 'affiliation')

        if affs:
            if type(affs) == dict:
                affs = [ affs ]
            if len(affs):
                return [ SCP_get_aff(aff) for aff in affs ]
        return []
    
    def SCP_get_authors_list(self):
        def SCP_get_author(au_):
            aff_id = get_data(au_, 'affiliation')
            if type(aff_id) == dict:
                aff_id = [ aff_id ]
                
            au = { 'au_id' : get_data(au_, '@auid'),           
                  'firstname' : get_data(au_, 'ce:given-name'),
                  'lastname' : get_data(au_, 'ce:surname'),
                  'fullname' : get_data(au_, 'ce:indexed-name'),
                  'aff_id' : aff_id,
                  'orcid' : None,
                  'email' : None
                 }
            return au

        def SCP_get_author_from_item(data):
            data = get_data(data, 'item')
            data = get_data(data, 'bibrecord')
            data = get_data(data, 'head')
            return get_data(data, 'author-group')
            # return get_data(data, 'author')

        aus = get_data(self.doc.data, 'authors')
        if not aus:
            aus = SCP_get_author_from_item(self.doc.data)

        if aus:
            aus =  [ aus['author'] ] if type(aus['author']) == dict else aus['author']

            if len(aus):
                return [ SCP_get_author(au) for au in aus ]
        return []

    def SCP_get_subjects(self):
        subj = get_data(get_data(self.doc.data, 'subject-areas'), 'subject-area')
        sbjs = []
        if subj:
            for sj in subj:
                sbjs.append(get_data(sj, '$'))
            return sbjs
        return None


    def SCP_get_keywords_list(self):
        keywords_ = {'author_keywords' : [], 'indexed_keywords' : []}

        authkeys = get_data(get_data(self.doc.data, 'authkeywords'), 'author-keyword')
        if authkeys:
            if type(authkeys) == dict:
                authkeys =  [ authkeys ]
            for key_ in authkeys:
                if get_data(key_, '$'):
                    keywords_['author_keywords'].append(get_data(key_, '$'))

        indexedkeys = get_data(get_data(self.doc.data, 'idxterms'), 'mainterm')
        if indexedkeys:
            if type(indexedkeys) == dict:
                indexedkeys = [ indexedkeys ]
            for key_ in indexedkeys:
                if get_data(key_, '$'):
                    keywords_['indexed_keywords'].append(get_data(key_, '$'))

        return keywords_

    def find_paper_links(self, crawlrefer=True, crawlcitations=False):
        from paper_link import Get_references, Get_citations
        if crawlrefer:
            self.references = Get_references(self.refs)
        if crawlcitations:
            self.citations = Get_citations(self.eid, self.scp_id)
        return self.references, self.citations

    def full_to_half(self, line):
        # return : 전각->반각 특수문자 통일된 제목 및 DB query를 위한 qtitle
        qtitle = None
        Special_Char = Qtitle.get_Special_Char()
        if line.strip() != '':
            qtitle = line.strip()
            for i,j in zip(Special_Char['full'], Special_Char['half']):
                line = line.replace(i, j)
                qtitle = qtitle.replace(i, '').replace(j, '')
            return line, qtitle.upper()
        else:
            return line, qtitle

    def get_scp_id(self):
        return self.scp_id

    def get_arXiv_id(self):
        return self.arXiv_id

    def get_title_year(self):
        return self.qtitle

    def to_json(self):
        """
        { "src" : value } ? 
       affiliation,headings,subheadings,subjects,refers,citations
        """
        src = self.src        
        data = dict()
        data['title'] = { src : self.title }
        data['doi'] = self.doi
        data['scp_id'] = self.scp_id
        data['arXiv_id'] = self.arXiv_id
        data['IEEE_id'] = self.IEEE_id
        data['uid'] = self.uid
        data['ISSN'] = self.ISSN
        data['scp_eid'] = self.eid
        data['abstract'] = self.abstract
        data['version'] = { src : self.version }
        data['start_date'] = { src : self.start_date }
        data['end_date'] = { src : self.end_date }
        data['subType'] = { src : self.subType }
        data['aggType'] = { src : self.aggType }
        data['bibText'] = { src : self.bibText }
        data['n_cite'] = { src : self.n_cite }
        data['n_ref'] = { src : self.n_ref }
        data['publisher'] = { src : self.publisher }
        data['publication'] = { src : self.publication }
        data['vol'] = { src : self.vol }
        data['issue'] = { src : self.issue }
        data['pages'] = { src : self.pages }
        data['affiliation'] = { src : self.affiliation }
        data['authors'] = { src : self.authors }
        data['keywords'] = { src : self.keywords }
        data['refers'] = { src : self.references }
        data['pub_year'] = { src : self.pub_year }
        data['citations'] = { src : self.citations }
        data['headings'] = { src : self.headings }
        data['subheadings'] = { src : self.subheadings }
        data['subjects'] = { src : self.subjects }
        for k in data.keys():
            data[k] = str(data[k])
        return data

    def Update_paper(self, ori):
        """
            ori = list
        """        
        columns = self.columns[1:]
        ori = ori[1:]
        new_p = self.to_json()
        
        while len(ori) < len(columns):
            ori.append('None')

        for i, c in enumerate(columns):
            if str(new_p[c]) == 'None':
                continue

            # id list
            if c in ['doi','arXiv_id', 'IEEE_id', 'uid', 'scp_id', 'scp_eid', 'ISSN', 'abstract']:
                if str(ori[i]) == 'None':
                    ori[i] = new_p[c]
                continue

            evalnew = literal_eval(new_p[c])
            if evalnew.get(self.src, None) == None:
                continue

            evalnew = get_data(evalnew, self.src)

            evalori = literal_eval(ori[i])
            oridata = get_data(evalori, self.src)
            if c in ['n_cite', 'n_ref']:
                if str(evalnew).isdigit():
                    if str(oridata).isdigit():
                        evalori[self.src] = max(int(evalnew), int(oridata))
                        ori[i] = str(evalori)
                    elif str(oridata) == 'None':
                        evalori[self.src] = evalnew
                        ori[i] = str(evalori)
                continue

            if c == 'keywords':
                for k in evalnew.keys():
                    if type(evalnew[k]) == str:
                        evalnew[k] = literal_eval(evalnew[k])
                    if type(oridata[k]) == str:
                        oridata[k] = literal_eval(oridata[k])
                    if len(evalnew[k]):
                        oridata[k].extend(evalnew[k])
                        oridata[k] = list(set(oridata[k]))
                evalori[self.src] = oridata
                ori[i] = str(evalori)
                continue

            if c=='authors':
                if str(oridata) == '[]' and str(evalnew) != '[]':
                    evalori[self.src] = evalnew
                    ori[i] = str(evalori)
            else:
                if str(oridata) == 'None' or str(oridata) == '[]':
                    if evalori == None:
                        evalori = dict()
                    evalori[self.src] = evalnew
                    ori[i] = str( evalori )
                    continue

                if (type(oridata) == tuple or type(oridata) == list) and len(oridata) > 0:
                    if str(oridata[0])=='None':
                        if evalori == None:
                            evalori = dict()
                        evalori[self.src] = evalnew
                        ori[i] = str( evalori )
        return ori

    def to_insert_sql(self):
        sql = "INSERT INTO paper (" + ','.join( i for i in self.columns[1:]) +') VALUES (' +','.join( '%s' for _ in self.columns[1:]) + ')'
        p = self.to_json()
        data = tuple( [ p[c] for c in self.columns[1:] ] )
        return sql, data

    def to_update_sql(self, p_id, datalist):
        sql = "UPDATE paper SET " + ','.join( c+'=%s' for i, c in enumerate(self.columns[1:]) ) + " WHERE p_id = %s"
        data = tuple(datalist + [p_id])
        return sql, data



