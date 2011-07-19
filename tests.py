import unittest
from sqlalchemy import Column, Integer, String, ForeignKey, Date, create_engine
from sqlalchemy.orm import Session, relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr
import datetime

from sqlalchemy_django_query import DjangoQuery


class BasicTestCase(unittest.TestCase):

    def setUp(self):
        class Base(object):
            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()
            id = Column(Integer, primary_key=True)
        Base = declarative_base(cls=Base)

        class Blog(Base):
            name = Column(String)
            entries = relationship('Entry', backref='blog')

        class Entry(Base):
            blog_id = Column(Integer, ForeignKey('blog.id'))
            pub_date = Column(Date)
            headline = Column(String)
            body = Column(String)

        engine = create_engine('sqlite://')
        Base.metadata.create_all(engine)
        self.session = Session(engine, query_cls=DjangoQuery)
        self.Base = Base
        self.Blog = Blog
        self.Entry = Entry
        self.engine = engine

        self.b1 = Blog(name='blog1', entries=[
            Entry(headline='b1 headline 1', body='body 1',
                  pub_date=datetime.date(2010, 2, 5)),
            Entry(headline='b1 headline 2', body='body 2',
                  pub_date=datetime.date(2010, 4, 8)),
            Entry(headline='b1 headline 3', body='body 3',
                  pub_date=datetime.date(2010, 9, 14))
        ])
        self.b2 = Blog(name='blog2', entries=[
            Entry(headline='b2 headline 1', body='body 1',
                  pub_date=datetime.date(2010, 5, 12)),
            Entry(headline='b2 headline 2', body='body 2',
                  pub_date=datetime.date(2010, 7, 18)),
            Entry(headline='b2 headline 3', body='body 3',
                  pub_date=datetime.date(2011, 8, 27))
        ])

        self.session.add_all([self.b1, self.b2])
        self.session.commit()

    def test_basic_filtering(self):
        bq = self.session.query(self.Blog)
        eq = self.session.query(self.Entry)
        assert bq.filter_by(name__exact='blog1').one() is self.b1
        assert bq.filter_by(name__contains='blog').all() == [self.b1, self.b2]
        assert bq.filter_by(entries__headline__exact='b2 headline 2').one() is self.b2
        assert bq.filter_by(entries__pub_date__range=(datetime.date(2010, 1, 1),
            datetime.date(2010, 3, 1))).one() is self.b1
        assert eq.filter_by(pub_date__year=2011).one() is self.b2.entries[2]
        assert eq.filter_by(pub_date__year=2011, id=self.b2.entries[2].id
            ).one() is self.b2.entries[2]

    def test_basic_excluding(self):
        eq = self.session.query(self.Entry)
        assert eq.exclude_by(pub_date__year=2010).one() is self.b2.entries[2]

    def test_basic_ordering(self):
        eq = self.session.query(self.Entry)
        assert eq.order_by('-blog__name', 'id').all() == \
            self.b2.entries + self.b1.entries


if __name__ == '__main__':
    unittest.main()
