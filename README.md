# vgo
GOLANG validator package, simply authorize json bodies, suitable for web api's

**Installation:**
```bash
go get -u github.com/xeuus/vgo
```

**Usage:**
```go


func test() {
    
    jsonBody := `{
        "name": "Hello World!",
        "username": "hello-world",
        "password": "123"
    }`

    result, err := vgo.Validate(jsonBody, []string{
        "name(string) required min(5)",
        "username(string) required username",
        "password(string) min(3)",
    })
    
    if err != nil {
        if values != nil {
            failedWithMessages(res, map[string]interface{}{
                "messages": result,
            })
            return
        }
        malformedInput(err)
    }
    succedResult(result)
}


```