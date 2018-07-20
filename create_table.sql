CREATE TABLE articles (
  arXiv_id VARCHAR(255) NOT NULL,
  doi VARCHAR(255),
  title TEXT NOT NULL,
  authors TEXT NOT NULL,
  abstract LONGTEXT NOT NULL,
  start_date VARCHAR(255) NOT NULL,
  pub_year CHAR(4) NOT NULL,
  publication TEXT,
  version VARCHAR(255),
  PRIMARY KEY (arXiv_id)
);
