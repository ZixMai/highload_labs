# Ход работы
- Мне было интересно как быстрее всего увидеть основные сущности, поэтому я немного поиграл с данными в jupyter'е [EDA.ipynb](EDA.ipynb)
- Затем аккуратненько раскидал всё в dbdiagram.io, результат: [dbml/mapping.dbml](dbml/mapping.dbml) и [dbml/schema.dbml](dbml/schema.dbml)
- Оттуда же DDL: [sql/0002_ddl.sql](sql/0002_ddl.sql)
- Заполнил БДшку и добавил функцию для учёта источника данных, чтобы хоть как-то различать то, что в моке: [sql/0001_pull_files.sql](sql/0001_pull_files.sql)
- DML: [sql/0003_dml.sql](sql/0003_dml.sql)
- Валидация работы: [validation.ipynb](validation.ipynb)