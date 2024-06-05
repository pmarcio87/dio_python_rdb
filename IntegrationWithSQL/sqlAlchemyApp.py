from sqlalchemy.orm import Session, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, inspect, select, func

Base = declarative_base()


class User(Base):
    __tablename__ = "user_account"

    # atributos
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    fullname = Column(String)

    address = relationship(
        "Address", back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"User (id={self.id}, name={self.name}, fullname={self.fullname})"


class Address(Base):
    __tablename__ = "address"

    # atributos
    id = Column(Integer, primary_key=True, autoincrement=True)
    email_address = Column(String(40), nullable=False)
    user_id = Column(Integer, ForeignKey("user_account.id"), nullable=False)

    user = relationship(
        "User", back_populates="address"
    )

    def __repr__(self):
        return f"Address (id={self.id}, email={self.email_address})"


# conexao com banco de dados
engine = create_engine("sqlite://")

# criando classes como tabelas no banco de dados
Base.metadata.create_all(engine)

# investiga o esquema do banco de dados
inspetor_engine = inspect(engine)


print(inspetor_engine.has_table("user"))
print(inspetor_engine.get_table_names())
print(inspetor_engine.default_schema_name)

# instanciando objetos no BD
with Session(engine) as session:
    paulo = User(
        name="Paulo",
        fullname="Paulo Mendes",
        address=[Address(email_address="paulo@email.com")]
        )
    
    carol = User(
        name="Carol",
        fullname="Carolina Silva",
        address=[Address(email_address="carolina@email.com"),
                 Address(email_address="carolsilva@email.org")]
    )

    jorge = User(
        name="Jorge",
        fullname="Jorge Silva",
        address=[Address(email_address="jorge@email.com")]
    )

    claudia = User(
        name="Claudia",
        fullname="Claudia Silva",
        address=[Address(email_address="claudia@email.com")]
    )

    dede = User(
        name="Dede",
        fullname="Andre Silva",
        address=[Address(email_address="dede@email.com")]
    )

    zaza = User(
        name="Zaza",
        fullname="Isaias Silva",
        address=[Address(email_address="zaza@email.com")]
    )


    # enviando para o BD (persistindo os dados)
    session.add_all([paulo, carol, jorge, claudia, dede, zaza])
    
    session.commit()


# Recuperando usuários a partir de uma query pelo nome
stmt = select(User).where(User.name.in_(["Cesar", "Paulo"]))

print(stmt) # Imprimindo query

for user in session.scalars(stmt):
    print(user)


# Recupearando emails a partir de uma query pela chave estrangeira
stmt_address = select(Address).where(Address.user_id.in_([2]))

print(stmt_address) # Imprimindo query

for address in session.scalars(stmt_address):
    print(address)

# Aplicando ORDER BY DESC na query
stmt_order = select(User).where(User.id > 2).order_by(User.name.desc())

print(stmt_order) # Imprimindo query

for user in session.scalars(stmt_order):
    print(user)

# Aplicando JOIN
stmt_join = select(User.fullname, Address.email_address).join_from(Address, User)

print(stmt_join) # Imprimindo query

for result in session.scalars(stmt_join): 
    print(result)

# Escalar pega apenas o primeiro resultado, portanto precisamos executar e fazer um fetchall()
connection = engine.connect()
results = connection.execute(stmt_join).fetchall()

print("Executando statement")
for result in results:
    print(result)

# Utilizando COUNT
stmt_count = select(func.count("*")).select_from(User)

print(stmt_count) # Imprimindo query

for count in session.scalars(stmt_count):
    print(count)

# Encerrando a sessão
session.close()