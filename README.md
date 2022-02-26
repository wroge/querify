# querify

Query your data from and to any json compatible source.
The query language used is similar to SQL with Postgres dialect.

```go
hobbiesTable := []map[string]interface{}{
    {"id": 1, "name": "Football"},
    {"id": 2, "name": "Basketball"},
    {"id": 3, "name": "Hockey"},
}

usersTable := []map[string]interface{}{
    {"id": 1, "name": "Max"},
    {"id": 2, "name": "Tom"},
    {"id": 3, "name": "Alex"},
}

userHobbiesTable := []map[string]interface{}{
    {"user_id": 1, "hobby_id": 1},
    {"user_id": 1, "hobby_id": 2},
    {"user_id": 2, "hobby_id": 3},
    {"user_id": 3, "hobby_id": 1},
}

type User struct {
    Name    string
    Hobbies []string
}

var users []User

err := querify.From(usersTable).As("users").
    Join(
        querify.LeftJoin{
            Right: querify.From(userHobbiesTable).As("user_hobbies"),
            On:    querify.Equals{querify.Ident("users.id"), querify.Ident("user_hobbies.user_id")},
        },
        querify.LeftJoin{
            Right: querify.From(hobbiesTable).As("hobbies"),
            On:    querify.Equals{querify.Ident("hobbies.id"), querify.Ident("user_hobbies.hobby_id")},
        },
    ).
    GroupBy(querify.Ident("users.name")).
    Select(
        querify.As{
            Name:       "name",
            Expression: querify.Ident("users.name"),
        },
        querify.As{
            Name: "hobbies",
            Expression: querify.ArrayAgg{
                Expression: querify.Ident("hobbies.name"),
            },
        },
    ).Scan(&users)
if err != nil {
    panic(err)
}

fmt.Println(users)
// [{Max [Football Basketball]} {Tom [Hockey]} {Alex [Football]}]
```

## Features

- Expression:
  - Literal
  - Ident
  - ArrayAgg
  - Concat
  - CountAll
  - Count
  - As
- GroupBy:
  - Ident
  - Cube
  - GroupingSets
- Condition:
  - And
  - Or
  - Equals
  - Greater
  - Less
- OrderBy:
  - Asc
  - Desc
- Join:
  - LeftJoin
- Limit
- Offset

Your required SQL feature isn't yet supported?
Implement these [interfaces](https://github.com/wroge/querify/blob/master/interface.go) and create a merge request!

## Dependencies

- [tidwall/gjson](https://github.com/tidwall/gjson)
