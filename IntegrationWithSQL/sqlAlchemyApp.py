from sqlalchemy.orm import Session, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, inspect

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
        address=[Address(email_address="pmarcio87@hotmail.com")]
        )
    
    carol = User(
        name="Carolina",
        fullname="Carolina Mariotti",
        address=[Address(email_address="carol_mariotti@hotmail.com"),
                 Address(email_address="carol_mariotti@icloud.com")]
    )


# enviando para o BD (persistindo os dados)
session.add_all([paulo, carol])

session.commit()