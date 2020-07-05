# kotlin-cosmos - A simple Azure CosmosDB Client for kotlin

Kotlin-cosmos is a client for Azure CosmosDB 's SQL API (also called documentdb formerly). Which is an opinionated library aimed at ease of use for CRUD and find (aka. query).

## Background
* Microsoft's official Kotlin CosmosDB client  is for Android
* Microsoft's official Java CosmosDB client is verbose to use

## Disclaimer
* This is an alpha version, and features are focused to CRUD and find at present.

## Quickstart

### Add dependency

#### Maven

```xml
TODO
```

#### Gradle

```groovy
TODO
```

### Start programming 

```kotlin
import io.github.thunderz99.cosmos.Cosmos

data class User(val id:String, val firstName:String, val lastName:String )

fun main() {
    val db = Cosmos(System.getenv("YOUR_CONNECTION_STRING")).getDatabase("Database1")
    db.createIfNotExist("Collection1")
    db.upsert("Collection1", data = User("id011", "Tom", "Banks"))
  
    val users = db.find("Collection1", filter {
        "id" > "id010",
        "lastName" to "Banks" 
    })
}
```


## Examples

### Work with partitions 

```kotlin
//TODO

```

### Create database and collection from zero

```kotlin
//TODO

```

### CRUD

```kotlin
//TODO

```

## Special thanks
TODO