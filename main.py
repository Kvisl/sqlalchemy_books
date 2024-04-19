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


def get_shops(publisher_input):
    query = sq.select(Book.title, Shop.name, Sale.price, Sale.date_sale) \
        .select_from(Book) \
        .join(Publisher, Publisher.id == Book.publisher_id) \
        .join(Stock, Stock.book_id == Book.id) \
        .join(Shop, Shop.id == Stock.shop_id) \
        .join(Sale, Sale.stock_id == Stock.id) \


    if publisher_input.isdigit():
        results = session.execute(query.filter(Publisher.id == publisher_input)).all()
    else:
        results = session.execute(query.filter(Publisher.name == publisher_input)).all()

    for title, shop_name, sale_price, data_sale in results:
        print(f'{title: <30} | {shop_name: <10} | {sale_price: <8} | {data_sale.strftime("%d-%m-%Y")}')


if __name__ == '__main__':
    publisher_input = input("Enter the publisher's name or id: ")
    get_shops(publisher_input)

