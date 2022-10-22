CREATE TABLE [dbo].[Articles](
	id_article NVARCHAR(100) PRIMARY KEY NOT NULL,
	title NVARCHAR(MAX),
	year SMALLINT,
	n_citation_global INT,
    n_citation_local INT,
);
------
CREATE TABLE [dbo].[Fos](
	id INT PRIMARY KEY NOT NULL IDENTITY(1, 1),
	fos NVARCHAR(1000),
	article_id NVARCHAR(100) FOREIGN KEY REFERENCES Articles(id_article)
);
------
CREATE TABLE [dbo].[References](
	id INT PRIMARY KEY NOT NULL IDENTITY(1, 1),
	reference NVARCHAR(1000),
	article_id NVARCHAR(100) FOREIGN KEY REFERENCES Articles(id_article)
);
------
CREATE TABLE [dbo].[Keywords](
	id INT PRIMARY KEY NOT NULL IDENTITY(1, 1),
	keyword NVARCHAR(1000),
	article_id NVARCHAR(100) FOREIGN KEY REFERENCES Articles(id_article)
);
------
CREATE TABLE [dbo].[Abstract](
	id INT PRIMARY KEY NOT NULL IDENTITY(1, 1),
	abstract NVARCHAR(MAX),
	article_id NVARCHAR(100) FOREIGN KEY REFERENCES Articles(id_article)
);
------
CREATE TABLE [dbo].[Authors](
	id_author NVARCHAR(100) PRIMARY KEY NOT NULL,
	name NVARCHAR(1000)
);
------
CREATE TABLE [dbo].[ArticlesAuthors](
	id_article NVARCHAR(100) FOREIGN KEY REFERENCES Articles(id_article),
	id_author NVARCHAR(100) FOREIGN KEY REFERENCES Authors(id_author),
	CONSTRAINT [PK_ArticlesAuthors] PRIMARY KEY CLUSTERED (id_article, id_author)
);
------
CREATE TABLE [dbo].[Orgs](
	id INT PRIMARY KEY NOT NULL IDENTITY(1, 1),
	org NVARCHAR(1000),
	id_author NVARCHAR(100) FOREIGN KEY REFERENCES Authors(id_author)
);
------
CREATE TABLE [dbo].[Venues](
	id_venue NVARCHAR(100) PRIMARY KEY NOT NULL,
	name NVARCHAR(MAX)
);
------
CREATE TABLE [dbo].[ArticlesVenues](
	id_article NVARCHAR(100) FOREIGN KEY REFERENCES Articles(id_article),
	id_venue NVARCHAR(100) FOREIGN KEY REFERENCES Venues(id_venue),
    CONSTRAINT [PK_ArticlesVenues] PRIMARY KEY CLUSTERED (id_article, id_venue)
);
