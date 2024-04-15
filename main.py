import os
import json
import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from models import create_table, Publisher, Book, Shop, Stock, Sale


DB_USER = os.getenv('DB_USER', 'default_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'default_password')
DB_NAME = os.getenv('DB_NAME', 'default_db_name')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')


DSN = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sqlalchemy.create_engine(DSN)

create_table(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

publ = input("Enter the publisher's name: ")

query = sq.select(Book.title, Shop.name, Sale.price, Sale.date_sale) \
    .select_from(Book) \
    .join(Publisher, Publisher.id == Book.publisher_id) \
    .join(Stock, Stock.book_id == Book.id) \
    .join(Shop, Shop.id == Stock.shop_id) \
    .join(Sale, Sale.stock_id == Stock.id) \
    .where(Publisher.name == publ)

results = session.execute(query).all()
for result in results:
    print('{} | {} | {} | {}'.format(result[0], result[1], result[2], result[3].strftime('%d-%m-%Y')))
