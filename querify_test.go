package querify_test

import (
	"testing"

	"github.com/wroge/querify"
)

func Test(t *testing.T) {
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
		).OrderBy(querify.Asc{
		Expression: querify.Ident("users.name"),
	}).Offset(1).Limit(1).Scan(&users)
	if err != nil {
		t.Fatal(err)
	}

	if users[0].Name != "Max" {
		t.Fatal(users)
	}
}
