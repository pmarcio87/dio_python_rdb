from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey, create_engine, text

engine = create_engine("sqlite:///memory")

metadata_obj = MetaData()

user = Table(
    "user",
    metadata_obj,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_name", String, nullable=False),
    Column("email_address", String(40)),
    Column("nickname", String(60), nullable=False)
)

user_prefs = Table(
    "user_prefs",
    metadata_obj,
    Column("pref_id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("user.id"), nullable=False),
    Column("pref_name", String(40), nullable=False), 
    Column("pref_value", String(100))
)

# Imprimindo informações das tabelas do esquema
print(metadata_obj.tables)

# Imprimindo as tabelas do esquema
for table in metadata_obj.sorted_tables:
    print(table)

# Criando as tabelas no SQLite
metadata_obj.create_all(engine)


# Executando uma query
row1_insert = text("insert into user values('1', 'paulo', 'paulo@email.com', 'pm')")
row2_insert = text("insert into user values('2', 'carolina', 'carolina@email.com', 'carol')")
row3_insert = text("insert into user values('3', 'isaias', 'isaias@email.com', 'zaza')")

sql = text("select * from user")
with engine.connect() as connection:
    connection.execute(row1_insert)
    connection.execute(row2_insert)
    connection.execute(row3_insert)
    result = connection.execute(sql)
    connection.commit()

for row in result:
    print(row)

# Novo esquema nomeado: "bank"
metadata_db_obj = MetaData(schema="bank")

financial_info = Table(
    "financial_info",
    metadata_db_obj,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", String(100), nullable=False)
)

# Informação da Primary Key
print(financial_info.primary_key)
print(financial_info.constraints)
