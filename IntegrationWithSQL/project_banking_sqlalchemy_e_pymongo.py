# PARTE 1 - Implementando um Banco de Dados Relacional com SQLAlchemy

# Entregavel:
# - Aplicacao com a definicao de um esquema por meio de classes usando SQLAlchemy
# - Insercao de um conjunto de dados minimo para manipulacao de informacoes
# - Execucao de metodos de recuperacao de dados via SQLAlchemy

# Importando classes e funcoes
from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy import Column, Integer, String, ForeignKey, Float, create_engine, inspect, select, func

# Criando a Base
Base = declarative_base()

# Criando tabelas do BD

# tabela Clientes: id (int) / Nome (string) / cpf (string9) / endereco (string9)
class Cliente(Base):
    __tablename__ = "cliente"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String, nullable=False)
    cpf = Column(String(9), nullable=False)
    endereco = Column(String(20))

    conta = relationship("Conta", back_populates="cliente", cascade="all, delete-orphan")

    def __repr__(self):
        return f'Cliente (Id: {self.id} / Nome: {self.nome} / CPF:{self.cpf} / Endereco: {self.endereco})'

# tabela Conta: id (int) / tipo (string) / agencia (string) / num (int) / id_cliente (int) / saldo (decimal)
class Conta(Base):
    __tablename__ = "conta"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tipo = Column(String, nullable=False)
    agencia = Column(String, nullable=False)
    num = Column(Integer, nullable=False)
    id_cliente = Column(Integer, ForeignKey("cliente.id"), nullable=False)
    saldo = Column(Float, nullable=False)

    cliente = relationship("Cliente", back_populates="conta")

    def __repr__(self):
        return f'Conta (Id: {self.id} / Tipo: {self.tipo} / Agencia:{self.agencia} / Numero: {self.num} / Id Cliente: {self.id_cliente} / Saldo: {self.saldo})'
    
# Conectando com SQLite
engine = create_engine("sqlite://")

# Criando tabelas no BD
Base.metadata.create_all(engine)

# Inspecionando o esquema do BD
inspetor = inspect(engine)

# Recuperando informacoes das tabelas
print(inspetor.get_table_names())

# Inserindo dados no BD
with Session(engine) as session:
    user1 = Cliente(
        nome="paulo",
        cpf="123456789",
        endereco="Rua Tal 100",
        conta=[Conta(
            tipo="Corrente",
            agencia="001",
            num=10,
            saldo=100.0
        )]
    )

    user2 = Cliente(
        nome="carolina",
        cpf="987654321",
        endereco="Rua Dois 3",
        conta=[Conta(
            tipo="Corrente",
            agencia="001",
            num=11,
            saldo=200.0
        )]
    )

    user3 = Cliente(
        nome="joao",
        cpf="573698725",
        endereco="Rua de Baixo 56",
        conta=[Conta(
            tipo="Poupança",
            agencia="001",
            num=12,
            saldo=150.0
        )]
    )

    user4 = Cliente(
        nome="maria",
        cpf="942435756",
        endereco="Rua Dois 3",
        conta=[Conta(
            tipo="Corrente",
            agencia="001",
            num=13,
            saldo=200.0
        )]
    )

    # Persistindo os dados
    session.add_all([user1, user2, user3, user4])
    session.commit()

# Recuperando informações utilizando queries

connection = engine.connect()
# Informações dos clientes
stmt = select(Cliente.nome, Cliente.cpf)

result = connection.execute(stmt).fetchall()
for cliente in result:
    print(cliente)

# Contagem de clientes
stmt_count = select(func.count("*")).select_from(Cliente)

for count in session.scalars(stmt_count):
    print(count)

# Nomes e saldos de clientes cujo saldo é maior que 125, ordenado por nome
stmt = select(Cliente.nome, Conta.saldo).join_from(Conta, Cliente).where(Conta.saldo>125).order_by(Cliente.nome)

result = connection.execute(stmt).fetchall()
for cliente in result:
    print(cliente)

# Encerrando a sessão
session.close()


# PARTE 2 - Implementando um Banco de Dados NoSQL com Pymongo

# Operações:
# - Conectar ao mongo atlas e criar um banco de dados
# - Definir uma coleção bank para criar os documentos dos clientes
# - Inserir documentos com a estrutura acima
# - Escrever instruções de recuperação de informações com base nos pares de chave e valor

import pprint
import pymongo as pym
import datetime as dt

client = pym.MongoClient("pymongo-url")

db = client.banking
collection = db.bank_collection

# Visualizando se a nova coleção foi criada
print(db.list_collection_names())

# Criando documentos dos usuários
new_users=[{
    "id":1,
    "nome":"paulo",
    "cpf":"123456789",
    "endereco":"Rua Tal 100",
    "tipo_conta":"Corrente",
    "agencia":"001",
    "num_conta":10,
    "saldo":100.0,
    "tags":["paulo","Rua Tal 100","Corrente"]
    },
    {
    "id":2,
    "nome":"carolina",
    "cpf":"987654321",
    "endereco":"Rua Dois 3",
    "tipo_conta":"Corrente",
    "agencia":"001",
    "num_conta":11,
    "saldo":200.0,
    "tags":["carolina","Rua Dois 3","Corrente"]
    },
    {
    "id":3,
    "nome":"joao",
    "cpf":"573698725",
    "endereco":"Rua de Baixo 56",
    "tipo_conta":"Poupança",
    "agencia":"001",
    "num_conta":12,
    "saldo":150.0,
    "tags":["joao","Rua de Baixo 56","Poupança"]
    },
    {
    "id":4,
    "nome":"maria",
    "cpf":"942435756",
    "endereco":"Rua Dois 3",
    "tipo_conta":"Corrente",
    "agencia":"001",
    "num_conta":13,
    "saldo":200.0,
    "tags":["maria","Rua Dois 3","Corrente"]
    }]

# Fazendo um bulk insert dos documentos
new_posts_id = collection.insert_many(new_users).inserted_ids
print(new_posts_id)

# Contando o número de documentos no DB
print(collection.count_documents({}))

# Contando o número de contas = "Corrente"
print(collection.count_documents({"tipo_conta":"Corrente"}))

# Recuperando documentos e ordenando pelo nome de forma decrescente
for cliente in collection.find({}).sort("nome",pym.DESCENDING):
    pprint.pprint(cliente)