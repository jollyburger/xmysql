## xmysql

xmysql is a simple MySQL client library, which provide some features as follows.

- cluster manager 
- read and write seperation with load balancing
- ORM model
- Reconnct machanism

---

[TOC]

## Prerequistion

```
go get -u github.com/go-sql-driver/mysql
```

## How to use

### Init xmysql instance

#### DSN for master DB

```
user:passwd@tcp(address:port)/database

```

#### DSN for slave DB

```
user1:passwd1@tcp(address1:port1)/db1|weight1;user2:passwd2@tcp(address2:port2)/db2|weight
```

### How to use

#### Register xmysql instance for different service

```
instance := xmysql.RegisterMysqlService({your_service}, {master_addr}, {backup_addr})
```

#### Query to DB

- Insert({service}, {sql}, {args})
- Update({service}, {sql}, {args})
- Delete({service}, {sql}, {args})
- Select({service}, {sql}, {args})
- Find({service}, {object}, {sql}, {args})
