package io.github.thunderz99.cosmos

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.microsoft.azure.documentdb.*
import mu.KotlinLogging
import org.json.JSONObject

private val log = KotlinLogging.logger {}

val mapper = ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).registerKotlinModule()

/**
 * class that represent a cosmos account
 *
 * Usage:
 * val account = Cosmos("AccountEndpoint=https://xxx.documents.azure.com:443/;AccountKey=xxx==;")
 * val db = account.getDatabase("Database1")
 *
 * //Then use db to do CRUD / query
 * db.upsert("Users", user)
 *
 */
class Cosmos(private val connectionString: String) {

    private val client: DocumentClient

    init {
        check(connectionString.contains("/;")) { "Make sure connectionString is in valid format: $connectionString" }
        check(connectionString.contains("https://")) { "Make sure connectionString is in valid format: $connectionString" }

        var (endpoint, key) = connectionString.split(";")

        endpoint = endpoint.removePrefix("AccountEndpoint=")
        key = key.removePrefix("AccountKey=")

        check(endpoint.isNotBlank()) { "endpoint must be non-empty" }
        check(key.isNotBlank()) { "key must be non-empty" }

        log.info { "endpoint: $endpoint" }
        log.info { "key: ${key.substring(0, 3)}..." }

        client = DocumentClient(
                endpoint, key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session
        )
    }

    /**
     * Get a CosmosDatabase object by name
     */
    fun getDatabase(database: String): CosmosDatabase {
        check(database.isNotBlank()) { "database must be non-empty" }
        return CosmosDatabase(client, database)
    }

    /**
     * Create the db and coll if not exist. Coll creation will be skipped is empty.
     */
    fun createIfNotExist(database: String, collection: String = ""): CosmosDatabase {

        if (collection.isBlank()) {
            createDatabaseIfNotExist(client, database)
        } else {
            createCollectionIfNotExist(client, database, collection)
        }

        return CosmosDatabase(client, database)
    }
}

/**
 * class that represent a cosmos database
 *
 * Usage:
 * val cosmos = Cosmos("AccountEndpoint=https://xxx.documents.azure.com:443/;AccountKey=xxx==;")
 * val db = cosmos.getDatabase("Database1")
 *
 * //Then use db to do CRUD / query
 * db.upsert("Users", user1)
 * user2: User = db.read("Users", "id001").toObject()
 *
 */
class CosmosDatabase(private val client: DocumentClient, private val database: String) {

    fun create(collection: String, data: Any, partition: String = collection): CosmosDocument {

        checkCollection(collection)

        val objectMap: MutableMap<String, Any> = mapper.convertValue(data)

        // add partition info
        objectMap[getDefaultPartitionKey()] = partition

        val collectionLink = getCollectionLink(database, collection)

        val resource = client.createDocument(
                collectionLink,
                objectMap,
                requestOptions(partition),
                false
        ).resource

        log.info { "created Document: $collectionLink/docs/${objectMap["id"]}, partition:$partition" }

        return CosmosDocument(resource.toObject(JSONObject::class.java))
    }

    /**
     * Throw 404 Not Found Exception if object not exist
     */
    fun read(collection: String, id: String, partition: String = collection): CosmosDocument {

        check(id.isNotEmpty()) { "id must not be empty" }
        checkCollection(collection)

        val documentLink = getDocumentLink(database, collection, id)

        val resource = client.readDocument(documentLink, requestOptions(partition)).resource
                .also {
                    log.trace { "readDocument: $documentLink, partition:$partition" }
                }

        return CosmosDocument(resource.toObject(JSONObject::class.java))
    }

    /**
     * Update existing data. if not exist, throw Not Found Exception
     */
    fun update(collection: String, id: String, data: Any, partition: String = collection): CosmosDocument {

        check(id.isNotEmpty()) { "id must not be empty" }
        checkCollection(collection)

        val documentLink = getDocumentLink(database, collection, id)

        val objectMap: MutableMap<String, Any> = mapper.convertValue(data)

        // add partition info
        objectMap[getDefaultPartitionKey()] = partition

        val resource = client.replaceDocument(documentLink, objectMap, requestOptions(partition)).resource
        log.info { "updated Document: $documentLink, partition:$partition" }

        return CosmosDocument(resource.toObject(JSONObject::class.java))
    }

    /**
     * upsert data (if not exist, create the data. if already exist, update the data)
     */
    fun upsert(collection: String, id: String = "", data: Any, partition: String = collection): CosmosDocument {

        checkCollection(collection)

        val collectionLink = getCollectionLink(database, collection)

        val objectMap: MutableMap<String, Any> = mapper.convertValue(data)

        // add partition info
        objectMap[getDefaultPartitionKey()] = partition

        if (id.isNotBlank()) {
            objectMap["id"] = id
        }

        check(objectMap["id"].toString().isNotEmpty()) { "id must be non-empty" }

        val resource = client.upsertDocument(collectionLink, objectMap, requestOptions(partition), true).resource
        log.info { "upserted Document: $collectionLink/docs/$id, partition:$partition" }

        return CosmosDocument(resource.toObject(JSONObject::class.java))
    }

    /**
     * Delete document. Do nothing if object not exist (Suppressing 404 Not Found Exception)
     */
    fun delete(coll: String, id: String, partition: String = coll) {

        check(id.isNotEmpty()) { "id must not be empty" }
        checkCollection(coll)

        val documentLink = getDocumentLink(database, coll, id)

        try {
            client.deleteDocument(documentLink, requestOptions(partition))
            log.info { "deleted Document: $documentLink, partition:$partition" }
        } catch (de: DocumentClientException) {
            if (!isResourceNotFoundException(de)) {
                throw de
            }
            log.info { "delete is skipped due to not found. Document: $documentLink, partition:$partition" }
        }
    }

    /**
     * find items from collection
     *
     * Usage:
     * <code>
     * val users = db.find("Data", filter = json {
     *    "name" to "Hanks"
     * }).toList<User>()
     *
     * // or
     *
     * val users = db.find(
     *   "Data",
     *   filter = json {
     *     "fullName.last" to "Hanks"
     *     "id" to listOf("id001", "id003", "id005")
     *   },
     *   sort = json {
     *     "_ts" to "DESC"
     *   },
     *   offset = 0,
     *   limit = 10,
     *   partition = "Users"
     * ).toList<User>()
     * </code>
     *
     *
     */
    fun find(
            collection: String,
            filter: JSONObject = JSONObject(),
            sort: JSONObject = JSONObject(""" {"_ts": "DESC"} """),
            offset: Int = 0,
            limit: Int = 100,
            partition: String = collection
    ): CosmosDocumentList {

        checkCollection(collection)

        val collectionLink = getCollectionLink(database, collection)

        val options = FeedOptions()
        options.partitionKey = PartitionKey(partition)

        val querySpec: SqlQuerySpec = buildQuerySpec(filter, sort, offset, limit)

        val docs = client.queryDocuments(
                collectionLink,
                querySpec,
                options
        ).queryIterable.toList<Document>()

        log.info { "find Document: collection: $collection, filter: $filter, sort: $sort, offset: $offset, limit: $limit,  partition:$partition" }

        val jsonObjs: List<JSONObject> = docs.asSequence().map { it.toObject(JSONObject::class.java) }.toList()

        return CosmosDocumentList(jsonObjs)
    }

    /**
     * return count of documents
     */
    fun count(
            collection: String,
            filter: JSONObject = JSONObject(),
            partition: String = collection
    ): Int {

        checkCollection(collection)

        val collectionLink = getCollectionLink(database, collection)

        val options = FeedOptions()
        options.partitionKey = PartitionKey(partition)

        val querySpec: SqlQuerySpec = buildQuerySpec(filter, count = true)

        val docs = client.queryDocuments(
                collectionLink,
                querySpec,
                options
        ).queryIterable.toList<Document>()

        return docs[0].getInt("$1")
    }

    protected fun requestOptions(partition: String): RequestOptions {
        val options = RequestOptions()
        options.partitionKey = PartitionKey(partition)
        return options
    }

    internal fun checkCollection(coll: String) {
        check(coll.isNotBlank()) { "coll must be non-blank" }
    }

    /**
     * Create the db and coll if not exist. Coll creation will be skipped is empty.
     */
    fun createIfNotExist(db: String, coll: String = ""): CosmosDatabase {

        if (coll.isBlank()) {
            createDatabaseIfNotExist(client, db)
        } else {
            createCollectionIfNotExist(client, db, coll)
        }

        return this
    }
}

/**
 * Represent a CosmosDB document. Has a JSONObject inside.
 *
 * Having toObject and toJson util method to convert to Class or String conveniently.
 *
 */
data class CosmosDocument(val jsonObj: JSONObject) {

    inline fun <reified T> toObject(): T {
        return mapper.readValue(jsonObj.toString())
    }

    fun toJson(): String {
        return mapper.writeValueAsString(jsonObj)
    }

    fun toMap(): Map<String, Any> {
        return mapper.readValue(jsonObj.toString())
    }
}

/**
 * Represent a list of CosmosDB document.
 *
 * Having toList and toJson util method to convert to List<T> or String conveniently.
 *
 */
data class CosmosDocumentList(val jsonObjs: List<JSONObject>) {

    inline fun <reified T> toList(): List<T> {
        return jsonObjs.asSequence().map { mapper.readValue<T>(it.toString()) }.toList()
    }

    fun toJson(): String {
        return mapper.writeValueAsString(jsonObjs)
    }
}


internal fun getDatabaseLink(db: String) = "/dbs/%s".format(db)

internal fun getCollectionLink(db: String, coll: String) =
    "/dbs/%s/colls/%s".format(db, coll)

internal fun getDocumentLink(db: String, coll: String, id: String) =
    "/dbs/%s/colls/%s/docs/%s".format(db, coll, id)

internal fun isResourceNotFoundException(e: Exception): Boolean {
    if (e is DocumentClientException) {
        return e.statusCode == 404
    }
    return e.message?.contains("Resource Not Found") ?: false
}

/**
 *  A helper function to generate c.foo IN (...) queryText
 *
 *  INPUT: "parentId", "@parentId", ["id001", "id002", "id005"], params
 *  OUTPUT: "( c.parentId IN (@parentId__0, @parentId__1, @parentId__2) )",  and add paramsValue into params
 */
internal fun buildArray(
    key: String,
    paramName: String,
    paramValue: Iterable<IndexedValue<Any?>>,
    params: SqlParameterCollection
): String {
    return paramValue.joinToString(", ", prefix = " (c.$key IN (", postfix = "))") { (index, v) ->
        val paramNameIdx = "${paramName}__$index"
        params.add(SqlParameter(paramNameIdx, v))
        paramNameIdx
    }
}

internal fun buildQuerySpec(
    filter: JSONObject = JSONObject(),
    sort: JSONObject = JSONObject(),
    offset: Int = 0,
    limit: Int = 100,
    count: Boolean = false
): SqlQuerySpec {

    var select = if (count) "COUNT(1)" else "*"
    val queryText = StringBuilder("SELECT $select FROM c")
    val params = SqlParameterCollection()

    // filter
    filter.keys().withIndex().forEach { (index, k) ->
        if (index == 0) {
            queryText.append(" WHERE")
        } else {
            queryText.append(" AND")
        }

        // fullName.last -> @fullName__last
        val paramName = "@${k.replace(".", "__")}"
        val paramValue = filter[k]

        if (paramValue is Collection<*>) {
            // e.g ( c.parentId IN (@parentId__0, @parentId__1, @parentId__2) )
            queryText.append(buildArray(k, paramName, paramValue.withIndex(), params))
        } else if (paramValue is Array<*>) {
            queryText.append(buildArray(k, paramName, paramValue.withIndex(), params))
        } else {
            // other types
            queryText.append(" (c.$k = $paramName)")
            params.add(SqlParameter(paramName, paramValue))
        }
    }

    // sort
    if (!sort.isEmpty && !count) {
        queryText.append(
            sort.keys().asSequence()
                .joinToString(separator = ",", prefix = " ORDER BY", postfix = "") { k ->
                    val sortStr = sort[k].toString().toUpperCase()
                    check(sortStr == "DESC" || sortStr == "ASC") { "Order must be ASC or DESC. provided: $sortStr" }
                    " c.$k $sortStr"
                })
    }

    // offset and limit
    if (!count) {
        queryText.append(" OFFSET $offset LIMIT $limit")
    }

    return SqlQuerySpec(queryText.toString(), params).also { log.info { "queryText:$queryText" } }
}

internal fun createDatabaseIfNotExist(client: DocumentClient, db: String): Database {

    var database = readDatabase(client, db)
    if (database == null) {
        val dbObj = Database()
        dbObj.id = db
        val options = RequestOptions()
        options.offerThroughput = 400
        val result = client.createDatabase(dbObj, options)
        log.info { "created nosql database: $db" }
        database = result.resource
    }

    check(database != null)
    return database
}

internal fun readDatabase(client: DocumentClient, db: String): Database? {
    return try {
        check(db.isNotBlank()) { "db must be non-empty" }
        val res =
            client.readDatabase(getDatabaseLink(db), null)
        res.resource
    } catch (de: DocumentClientException) {
        // If not exist
        if (isResourceNotFoundException(de)) {
            null
        } else {
            // Throw any other Exception
            throw de
        }
    }
}

internal fun createCollectionIfNotExist(client: DocumentClient, db: String, coll: String): DocumentCollection {

    createDatabaseIfNotExist(client, db)

    var collection = readCollection(client, db, coll)

    if (collection == null) {

        val collectionInfo = DocumentCollection()
        collectionInfo.id = coll

        collectionInfo.indexingPolicy = getDefaultIndexingPolicy()
        collectionInfo.defaultTimeToLive = -1

        val partitionKeyDef = PartitionKeyDefinition()
        val paths: List<String> = listOf("/${getDefaultPartitionKey()}")
        partitionKeyDef.paths = paths

        collectionInfo.partitionKey = partitionKeyDef

        collection = client.createCollection(
            getDatabaseLink(db),
            collectionInfo,
            null
        ).resource
        log.info {
            "Created nosql collection. db:$db, coll: $coll"
        }
    }

    check(collection != null)
    return collection
}

internal fun readCollection(client: DocumentClient, db: String, coll: String): DocumentCollection? {
    return try {
        val res = client
            .readCollection(
                getCollectionLink(
                    db,
                    coll
                ), null
            )
        res.resource
    } catch (de: DocumentClientException) {
        // 存在しない場合
        if (de.statusCode == 404) {
            null
        } else {
            // チェックは失敗した場合はExceptionを投げる
            throw de
        }
    }
}

/**
 * return the default indexingPolicy
 */
internal fun getDefaultIndexingPolicy(): IndexingPolicy {
    val rangeIndexOverride =
        arrayOfNulls<Index>(3)
    rangeIndexOverride[0] = Index.Range(DataType.Number, -1)
    rangeIndexOverride[1] = Index.Range(DataType.String, -1)
    rangeIndexOverride[2] = Index.Spatial(DataType.Point)
    val indexingPolicy = IndexingPolicy(rangeIndexOverride)
    indexingPolicy.indexingMode = IndexingMode.Consistent
    log.info { "set indexing policy to default:$indexingPolicy " }
    return indexingPolicy
}

internal fun getDefaultPartitionKey(): String = "_partition"

