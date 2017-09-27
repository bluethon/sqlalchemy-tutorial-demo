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
print(Base.metadata.create_all(engine))

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

# === Querying
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
query.all()

# first() - one
query.first()

# one()
# fully fetches all rows,
user = query.one()

