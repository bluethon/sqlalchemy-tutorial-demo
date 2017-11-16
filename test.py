#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Date     : 2017-09-18 14:16
# @Author   : Bluethon (j5088794@gmail.com)
# @Link     : http://github.com/bluethon

# check version
import sqlalchemy

print(sqlalchemy.__version__)

# connecting
from sqlalchemy import create_engine

# sqlite, 内存模式, 只是存储使用内存, 不是内存数据库
# echo, 启用日志系统, 继承自Python的logging模块
# Lazy Connecting, 声明时并没有连接
engine = create_engine('sqlite:///:memory:', echo=True)

# Declare a Mapping
# 数据库表(tables) + 映射类(Mapping Class) = ORM
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# 定义至少需要:
# 1, `__tablename__`
# 2, one Column with primary key
from sqlalchemy import Column, Integer, String


class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    password = Column(String)
    
    def __repr__(self):
        return f'<User(name={self.name}, fullname={self.fullname}, password={self.password})>'


print('=== User table')
print(User.__table__)

print('=== Base create all')
Base.metadata.create_all(engine)

ed_user = User(name='ed', fullname='Ed Jones', password='edspassword')

print('=== User info')
print(ed_user.name)
print(ed_user.password)
print(ed_user.id)

from sqlalchemy.orm import sessionmaker

# 直接定义
Session = sessionmaker(bind=engine)


# 间接
def create_session():
    # 1 先声明
    Session = sessionmaker()
    # 2 使用create_engine()
    # 3 设置
    Session.configure(bind=engine)


# 使用session
# 此时Session关联一个Engine对象
# 但无连接, 第一次使用实例时, 从Engine的连接池取得
# 持续到commit所有修改 或 关闭session对象
session = Session()

# pending, no SQL
session.add(ed_user)

# 先插入, 再查询
our_user = session.query(User).filter_by(name='ed').first()
print(our_user)

print(ed_user is our_user)

session.add_all([
    User(name='wendy', fullname='Wendy Williams', password='foobar'),
    User(name='mary', fullname='Mary Contrary', password='xxg527'),
    User(name='fred', fullname='Fred Flinstone', password='blah'),
])

ed_user.password = 'f8s7ccs'

# 修改部分
print(session.dirty)

print(session.new)

# update & insert*3
session.commit()

# 重新先select一次
print(ed_user.id)

ed_user.name = 'Edwardo'

fake_user = User(name='fakeuser', fullname='Invalid', password='12345')

session.add(fake_user)

print('=== users')
print(session.query(User).filter(User.name.in_(['Edwardo', 'fakeuser'])).all())

session.rollback()

print(ed_user.name)
print(fake_user in session)

print('=== users2')
print(session.query(User).filter(User.name.in_(['Edwardo', 'fakeuser'])).all())

# --- Querying
for instance in session.query(User).order_by(User.id):
    print(instance.name, instance.fullname)

for name, fullname in session.query(User.name, User.fullname):
    print(name, fullname)

for row in session.query(User, User.name).all():
    print(row.User, row.name)

for row in session.query(User.name.label('name_label')).all():
    print(row.name_label)

# 别名
from sqlalchemy.orm import aliased

user_alias = aliased(User, name='user_alias')

for row in session.query(user_alias, user_alias.name).all():
    print(row.user_alias)

# limit 2 offset 1
for u in session.query(User).order_by(User.id)[1:3]:
    print(u)

# WHERE or filter
for name, in session.query(User.name).filter_by(fullname='Ed Jones'):
    print(name)

for name, in session.query(User.name).filter(User.fullname == 'Ed Jones'):
    print(name)

for user in session.query(User). \
        filter(User.name == 'ed'). \
        filter(User.fullname == 'Ed Jones'):
    print(user)

# ### Common Filter Operators
# equals
session.query(User).filter(User.name == 'ed')

# not equals
session.query(User).filter(User.name != 'ed')

# LIKE(大小写敏感与否因数据库而异)
session.query(User).filter(User.name.like('%ed%'))

# ILIKE(保证大小写!不!敏感) ignore
# 大部分数据库不支持, 此时, 通过 LIKE和LOWER SQL配合实现
session.query(User).filter(User.name.ilike('%ed%'))

# IN
session.query(User).filter(User.name.in_(['ed', 'wendy', 'jack']))
# works with query objects too:
session.query(User).filter(User.name.in_(
    session.query(User.name).filter(User.name.like('%ed%'))
))

# NOT IN
session.query(User).filter(~User.name.in_(['ed', 'wendy', 'jack']))

# IS NULL
session.query(User).filter(User.name == None)
# alternatively, if pep8/linters are a concern
session.query(User).filter(User.name.is_(None))

# AND
from sqlalchemy import and_

session.query(User).filter(and_(User.name == 'ed', User.fullname == 'Ed Jones'))
# or send multiple expressions to .filter()
session.query(User).filter(User.name == 'ed', User.fullname == 'Ed Jones')
# or chain multiple filter()/filter_by() calls
session.query(User).filter(User.name == 'ed').filter(User.fullname == 'Ed Jones')

# OR
from sqlalchemy import or_

session.query(User).filter(or_(User.name == 'ed', User.name == 'wendy'))

# MATCH
# 使用数据库的`MATCH`或`CONTAINS`函数, SQLite不支持
session.query(User).filter(User.name.match('wendy'))

# ### Returning Lists and Scalars
# all() - list
query = session.query(User).filter(User.name.like('%ed')).order_by(User.id)
print(query.all())

# first() - one
print(query.first())


# one()
# 按请求条件获取所有行, 如果行不唯一 or 返回数组(?) or 为空, 报错
def test_one():
    user = query.one()


# one_or_none()
# 与one()类似(返回多行时报错), 但是为空时不报错, 返回None
def test_one_or_none():
    user = query.one_or_none()


# scalar()
# 先调用one()方法, 成功后返回行的第一列值
query = session.query(User.id).filter(User.name == 'ed').order_by(User.id)
print(query.scalar())  # 1

# ### Using Textual SQL
from sqlalchemy import text

for user in session.query(User). \
        filter(text('id<224')). \
        order_by(text('id')).all():
    print(user.name)

res = session.query(User).filter(text('id<:value and name=:name')). \
    params(value=224, name='fred').order_by(User.id).one()
print(res)

res = session.query(User).from_statement(
    text('SELECT * FROM users WHERE name=:name')). \
    params(name='ed').all()
print(res)

stmt = text('SELECT name, id, fullname, password FROM users WHERE name=:name')
stmt = stmt.columns(User.name, User.id, User.fullname, User.password)
res = session.query(User).from_statement(stmt).params(name='ed').all()
print(res)

# 不返回User, 返回单列
stmt = text("SELECT name, id FROM users WHERE name=:name")
stmt = stmt.columns(User.name, User.id)
session.query(User.id, User.name).from_statement(stmt).params(name='ed').all()

# ### Counting
# count统计的是整个select的subquery
# 如果是select内count使用func.count()
res = session.query(User).filter(User.name.like('%ed')).count()
print(res)

# func.count()
from sqlalchemy import func

res = session.query(func.count(User.name), User.name).group_by(User.name).all()
print(res)

# count table
res = session.query(func.count('*')).select_from(User).scalar()
print(res)
# 如果使用列名, select_from()可省
session.query(func.count(User.id)).scalar()
session.query(func.count(User.name)).scalar()

# --- Building a Relationship
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship


class Address(Base):
    __tablename__ = 'addresses'
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    
    user = relationship('User', back_populates='addresses')
    
    def __repr__(self):
        return f'<Address(email_address={self.email_address})'


# 外部定义relationship
# order_by字段会影响所有join方式(subqueryload, joinedload),
# 手动的order_by不能用joinedload(因为表是匿名的), 只能用join
User.addresses = relationship('Address', order_by=Address.id, back_populates='user')

# 创建Address表, 会跳过已创建的表
Base.metadata.create_all(engine)

# --- Working with Related Objects
# collection的返回类型可以自定义, 默认是list
# > http://docs.sqlalchemy.org/en/latest/orm/collections.html#custom-collections
jack = User(name='jack', fullname='Jack Bean', password='gjffdd')
print(jack.addresses)

jack.addresses = [
    Address(email_address='jack@google.com'),
    Address(email_address='j25@yahoo.com'),
]

print(jack.addresses[1])

print(jack.addresses[1].user)

session.add(jack)
session.commit()

jack = session.query(User).filter_by(name='jack').one()

print(jack)

# 执行addresses表的SELECT, 返回list, lazy loading
# > http://docs.sqlalchemy.org/en/latest/glossary.html#term-lazy-loading
print(jack.addresses)

# --- Querying with Joins
# 不使用join, 但是等价
for u, a in session.query(User, Address). \
        filter(User.id == Address.user_id). \
        filter(Address.email_address == 'jack@google.com').all():
    print(u)
    print(a)

# join
res = session.query(User).join(Address). \
    filter(Address.email_address == 'jack@google.com').all()
print(res)
# multiple ForeignKey(多外键)
query.join(Address, User.id == Address.user_id)  # explicit condition
query.join(User.addresses)  # specify relationship from left to right
query.join(Address, User.addresses)  # same, with explicit target
query.join('addresses')  # same, using a string

# outer join
query.outerjoin(User.addresses)  # LEFT OUTER JOIN

# 返回多Model时, 选择 from表 和 on表
query = session.query(User, Address).select_from(Address).join(User)
print(query)

# ### Using Aliases
from sqlalchemy.orm import aliased

adalias1 = aliased(Address)
adalias2 = aliased(Address)
for username, email1, email2 in \
        session.query(User.name, adalias1.email_address, adalias2.email_address). \
                join(adalias1, User.addresses). \
                join(adalias2, User.addresses). \
                filter(adalias1.email_address == 'jack@google.com'). \
                filter(adalias2.email_address == 'j25@yahoo.com'):
    print(username, email1, email2)

# ### Using Subqueries
from sqlalchemy.sql import func

# ?? 类似使用了alias() ??
# shorthand for query.statement.alias()
# statement
stmt = session.query(Address.user_id, func.count('*'). \
                     label('address_count')). \
    group_by(Address.user_id).subquery()
print(stmt)

print('=== stmt.c')
# 使用类似Table结构, 列叫 `c`
for u, count in session.query(User, stmt.c.address_count). \
        outerjoin(stmt, User.id == stmt.c.user_id).order_by(User.id):
    print(u, count)

# ### Selecting Entities from Subqueries
stmt = session.query(Address). \
    filter(Address.email_address != 'j25@yahoo.com'). \
    subquery()
# 子查询匹配到Model
adalias = aliased(Address, stmt)
for user, address in session.query(User, adalias). \
        join(adalias, User.addresses):
    print(user)
    print(address)

print('### Using EXISTS')
from sqlalchemy.sql import exists

# v1
stmt = exists().where(Address.user_id == User.id)
for name, in session.query(User.name).filter(stmt):
    print(name)

# v2 any()
for name, in session.query(User.name). \
        filter(User.addresses.any()):
    print(name)

# v3 any(条件) 在any内过滤
for name, in session.query(User.name). \
        filter(User.addresses.any(Address.email_address.like('%google%'))):
    print(name)

# v4 has(), 此处~ == NOT
print(session.query(Address).
      filter(~Address.user.has(User.name == 'jack')).all())

print('### Common Relationship Operators')
someaddress = Address()
someuser = User()
# __eq__()
query.filter(Address.user == someuser)
# __ne__()
query.filter(Address.user != someuser)
# IS NULL
query.filter(Address.user == None)
# contains()
query.filter(User.addresses.contains(someaddress))
# any()
query.filter(User.addresses.any(Address.email_address == 'bar'))
query.filter(User.addresses.any(email_address='bar'))
# has()
query.filter(Address.user.has(name='ed'))
# Query.with_parent()
session.query(Address).with_parent(someuser, 'addresses')

print('=================')
print('--- Eager Loading')
print('=================')

print('### Subquery Load')
from sqlalchemy.orm import subqueryload

# 产生2个查询, User 和 User.id join Address
jack = session.query(User). \
    options(subqueryload(User.addresses)). \
    filter_by(name='jack').one()
print(jack)
print(jack.addresses)

print('### Joined Load')
# LEFT OUTER JOIN
# 1个查询, User join Address, 最正常的
# return 2 rows, 但是Query会合并
from sqlalchemy.orm import joinedload

jack = session.query(User). \
    options(joinedload(User.addresses)). \
    filter_by(name='jack').one()
print(jack)
print(jack.addresses)

# joinedload() & subqueryload()
# joinedload() 适合多对一的关系
# subqueryload() 适合查找相关的对象集合
# subqueryload() tends to be more appropriate for loading related collections
# while joinedload() tends to be better suited for many-to-one relationships,
# due to the fact that only one row is loaded for both the lead and the related object.

# joinedload() & join()
# joinedload(), 匿名别称, 不影响query结果, order_by()和filter()不能使用其列

# 这些load()主要是用来填充collection的, 也就是User.addresses的
# join()是用来query查询(where字段)的
# > http://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html#zen-of-eager-loading

print('### Explicit Join + Eagerload')
# 适用与多对一的多端(?)
from sqlalchemy.orm import contains_eager

jack_addresses = session.query(Address). \
    join(Address.user). \
    filter(User.name == 'jack'). \
    options(contains_eager(Address.user)). \
    all()
print(jack_addresses)
print(jack_addresses[0].user)

# > http://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html

print('============')
print('--- Deleting')
print('============')

# > http://docs.sqlalchemy.org/en/latest/orm/cascades.html#unitofwork-cascades
# > http://docs.sqlalchemy.org/en/latest/orm/collections.html#passive-deletes

# Address没有删除2 rows, 只是Address.user_id = NULL
session.delete(jack)
print(session.query(User).filter_by(name='jack').count())

print(session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count())

print('### Configuring delete/delete-orphan Cascade')
session.close()  # ROLLBACK

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    password = Column(String)
    
    addresses = relationship('Address', back_populates='user',
                             cascade='all, delete, delete-orphan')
    
    def __repr__(self):
        return f'<User(name={self.name}, fullname={self.fullname}, password={self.password})>'


class Address(Base):
    __tablename__ = 'addresses'
    
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    
    user = relationship('User', back_populates='addresses')
    
    def __repr__(self):
        return f'<Address(email_address={self.email_address})>'


jack = session.query(User).get(5)

# 下行只执行select语句
del jack.addresses[1]

# 此处执行query时, 先执行上面的Delete
print(session.query(Address).filter(
    Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count())

session.delete(jack)

print(session.query(User).filter_by(name='jack').count())
print(session.query(Address).filter(
    Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count())

print('========================================')
print('--- Building a Many To Many Relationship')
print('========================================')

from sqlalchemy import Table, Text

# association table
post_keywords = Table('post_keywords', Base.metadata,
                      Column('post_id', ForeignKey('posts.id'), primary_key=True),
                      Column('keyword_id', ForeignKey('keywords.id'), primary_key=True))


class BlogPost(Base):
    __tablename__ = 'posts'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    headline = Column(String(255), nullable=False)
    body = Column(Text)
