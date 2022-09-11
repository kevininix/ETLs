USE [master]
GO
CREATE LOGIN [etl] WITH PASSWORD=N'demopass', DEFAULT_DATABASE = [AdventureWorksDW2019]
GO
USE [AdventureWorksDW2019]
GO
CREATE USER [etl] FOR LOGIN [etl]
GO
USE [AdventureWorksDW2019]
GO
ALTER ROLE [db_datareader] ADD MEMBER [etl]
GO
USE [master]
GO
GRANT CONNECT SQL TO [etl]
GO