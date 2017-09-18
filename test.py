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
        return f'<User(name={self.name}, fullanme={self.fullname}, password={self.password})>'


print(User.__table__)

print(Base.metadata.create_all(engine))
