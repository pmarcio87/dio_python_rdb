import pprint
import pymongo as pym
import datetime as dt

client = pym.MongoClient("mongodb+srv://pmarcio87:123abc@cluster0.xyve0yi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db = client.test
collection = db.test_collection

# Criando um documento
post = {
    "author":"paulo",
    "text":"My first MongoDB app based on Python",
    "tags":["paulo", "mongodb", "python3", "pymongo"],
    "date":dt.datetime.now(dt.UTC)
}

# Subindo o documento para o DB
post_id = db.posts.insert_one(post).inserted_id

# Vendo o ID do documento
print(post_id)

# Recuperando os nomes das coleções no DB
print(db.list_collection_names())

# Recuperando a primeira ocorrência no DB
print(db.posts.find_one())
pprint.pprint(db.posts.find_one()) # Imprimindo de maneira indentada

# Criando dois documentos
new_posts = [{
    "author":"paulo",
    "text":"Second document using pymongo",
    "tags":["paulo", "second", "mongodb", "pymongo"],
    "date":dt.datetime.now(dt.UTC)},{
    "author":"carolina",
    "title":"New Author in Town!",
    "text":"Third document from a new author using pymongo",
    "tags":["third", "carolina", "pymongo", "new"],
    "date":dt.datetime(2024, 6, 6, 11, 2)}]

# Fazendo um bulk insert
new_posts_id = db.posts.insert_many(new_posts).inserted_ids
print(new_posts_id)

# Recuperando documentos do autor "paulo"
query = db.posts.find_one({"author":"paulo"}) # Recuperando a primeira instância
pprint.pprint(query)

query = db.posts.find({"author":"paulo"}) # Recuperando todos os documentos

for item in query:
    pprint.pprint(item)

# Contando o número de documentos no DB
print(db.posts.count_documents({}))

# Contando documentos do autor "carolina"
print(db.posts.count_documents({"author":"carolina"}))

# Contando documentos com a tag "mongodb"
print(db.posts.count_documents({"tags":"mongodb"}))

# Recuperando documentos e ordenando pela data em ordem descendente
for post in db.posts.find({}).sort("date", pym.DESCENDING):
    pprint.pprint(post)

# Criando índices únicos para cada autor
result = db.profiles.create_index([("author", pym.ASCENDING)], unique=True)

print(sorted(list(db.profiles.index_information())))

# Criando nova coleção com profiles para os usuários
user_profiles = [{
    "user_id": 211,
    "name":"zaza"},
    {
        "user_id":212,
        "name": "cesar"}]

db.user_profiles.insert_many(user_profiles)

# Visualizando se a nova coleção foi criada
print(db.list_collection_names())

# Recuperando profiles dentro da nova coleção
print([profile for profile in db.user_profiles.find({})])

# Removendo coleção do DB e visualizando
db.drop_collection("user_profiles") # alternativa: db.user_profiles.drop()
print(db.list_collection_names())

# Removendo documentos
db.posts.delete_one({"author":"carolina"}) # Utilizar o método .delete_many() para remover múltiplos
print(db.posts.find_one({"author":"carolina"})) # Confirmando que documento foi deletado da coleção

# Removendo o DB
client.drop_database("test")
for db in client.list_databases():
    print(db)