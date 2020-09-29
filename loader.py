import csv
import os
import sys
import time

import click
import sqlalchemy
from pymongo import MongoClient
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table,Column,Integer,String
from sqlalchemy.ext.declarative import declarative_base


class Loader:
    def __init__(self, path: str, uri: str, database: str):
        """
        :param path: 本地文件路径
        :param uri: 数据库连接uri
        :param database: 数据库名称
        """
        self.path = path
        self.uri = uri
        self.database = database
        if not os.path.exists(path):
            raise Exception('data does not exists ')
        f = open(self.path, 'r')
        self.f = f
        self.reader = csv.DictReader(f)

    def write(self):
        raise NotImplementedError

    def close(self):
        self.f.close()


class MySQLLoader(Loader):

    def __init__(self, path=None, uri=None, database=None):

        super().__init__(path, uri, database)
        self.engine = sqlalchemy.create_engine(f'{self.uri}/{self.database}',
                                               pool_recycle=10)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.table = None
        self._init_table()

    def close(self):
        super().close()
        self.session.close()

    # 创建数据表
    def _init_table(self):
        filename = os.path.basename(self.path).split('.')[0]
        Base = declarative_base()
        self.table = Table(filename, Base.metadata,
                           Column('id', Integer(), primary_key=True),
                           *(Column(column, String(32)) for column in self.reader.fieldnames))
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def write(self):
        tic = time.time()
        count = 0
        batch = []
        for row in self.reader:
            batch.append(row)
            count += 1
            if count % 1000 == 0:
                insert = self.table.insert().values(batch)
                self.session.execute(insert)
                self.session.commit()
                print(f'by current have loaded {count}')
                batch = []
        insert = self.table.insert().values(batch)
        self.session.execute(insert)
        self.session.commit()
        print(f'toc:{time.time() - tic}')


class MongoLoader(Loader):
    def __init__(self, filepath, uri, database):
        super().__init__(filepath, uri, database)
        self.client = MongoClient(uri)

    def close(self):
        super().close()
        self.client.close()

    def write(self):
        db = self.client[self.database]
        collection = os.path.basename(self.path).split('.')[0]
        db.drop_collection(collection)
        tic = time.time()
        docs = []
        count = 0
        for row in self.reader:
            docs.append(row)
            count += 1
            if count % 1000 == 0:
                db[collection].insert_many(docs)
                print(f'by current have loaded: {count}')
                docs = []
        db[collection].insert_many(docs)
        print(f'toc:{time.time() - tic}')


@click.group()
def cli():
    pass


@cli.command()
@click.option('--source', '-s', help='data source type')
@click.option('--filepath', '-f', help='local file to upload ')
@click.option('--uri', help='database uri')
@click.option('--database', '-d', help='database name ')
def load(source, filepath, uri, database):
    if source == 'mysql':
        loader = MySQLLoader(filepath, uri, database)
    elif source == 'mongo':
        loader = MongoLoader(filepath, uri, database)
    else:
        raise Exception('unsupported data source type')
    loader.write()
    loader.close()


if __name__ == '__main__':
    cli()
