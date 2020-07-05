package io.github.thunderz99.cosmos

import com.microsoft.azure.documentdb.DocumentClientException
import com.microsoft.azure.documentdb.SqlParameter
import com.systema.analytics.es.misc.json
import io.github.cdimascio.dotenv.dotenv
import mu.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

val dotenv = dotenv()

private val log = KotlinLogging.logger {}

data class FullName(val first: String, val last: String)

data class User(val name: String, val id: String, val fullName: FullName? = FullName("", ""))

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CosmosTest {
    private val db =
        CosmosAccount(dotenv.get("COSMOSDB_CONNECTION_STRING") ?: "").getDatabase("CosmosDB")

    @Test
    fun `Create and Read should work`() {

        val user = User("rai", "id001")

        db.delete("Data", user.id, "Users")

        val result: User = db.create("Data", user, "Users").toObject()

        try {
            assertThat(result.id).isEqualTo("id001")
            assertThat(result.name).isEqualTo("rai")

            val user1: User = db.read("Data", user.id, "Users").toObject()
            assertThat(user1.id).isEqualTo("id001")
            assertThat(user1.name).isEqualTo("rai")
        } finally {
            db.delete("Data", user.id, "Users")
        }
    }

    @Test
    fun `Upsert and Delete should work`() {

        val user = User("rai2", "id002")

        val result: User = db.upsert("Data", user.id, user, "Users").toObject()

        try {
            assertThat(result.id).isEqualTo(user.id)
            assertThat(result.name).isEqualTo(user.name)

            db.delete("Data", user.id, "Users")

            try {
                db.read("Data", user.id, "Users")
                fail<String>("should throw not found exception")
            } catch (e: DocumentClientException) {
                assertThat(e.statusCode).isEqualTo(404)
            }
        } finally {
            db.delete("Data", user.id, "Users")
        }
    }

    @Test
    fun `Update should work`() {

        val user = User("rai3", "id003")

        db.upsert("Data", user.id, user, "Users")

        val userUpdate = User("rai3_updated", "id003")

        val result: User = db.update("Data", userUpdate.id, userUpdate, "Users").toObject()

        try {
            assertThat(result.id).isEqualTo(userUpdate.id)
            assertThat(result.name).isEqualTo(userUpdate.name)

            val user1: User = db.read("Data", user.id, "Users").toObject()

            assertThat(user1.id).isEqualTo(userUpdate.id)
            assertThat(user1.name).isEqualTo(userUpdate.name)
        } finally {
            db.delete("Data", user.id, "Users")
        }
    }

    @Test
    fun `Update should throw Exception if not exist`() {

        val user = User("rai4", "id004")

        try {
            db.update("Data", user.id, user, "Users")
            fail<String>("should throw not found exception")
        } catch (e: DocumentClientException) {
            assertThat(e.statusCode).isEqualTo(404)
        } finally {
            db.delete("Data", user.id, "Users")
        }
    }

    @Test
    fun `buildQuerySpec should get correct SQL`() {
        val q = buildQuerySpec(
            filter = json {
                "fullName.last" to "Hanks"
                "id" to listOf("id001", "id002", "id005")
                "age" to 30
            },
            sort = json {
                "_ts" to "DESC"
            },
            offset = 10,
            limit = 20
        )

        assertThat(q.queryText.trim()).isEqualTo("""SELECT * FROM c WHERE (c.fullName.last = @fullName__last) AND (c.id IN (@id__0, @id__1, @id__2)) AND (c.age = @age) ORDER BY c._ts DESC OFFSET 10 LIMIT 20""")

        assertThat(q.parameters.elementAt(0).toJson()).isEqualTo(SqlParameter("@fullName__last", "Hanks").toJson())
        assertThat(q.parameters.elementAt(1).toJson()).isEqualTo(SqlParameter("@id__0", "id001").toJson())
        assertThat(q.parameters.elementAt(2).toJson()).isEqualTo(SqlParameter("@id__1", "id002").toJson())
        assertThat(q.parameters.elementAt(3).toJson()).isEqualTo(SqlParameter("@id__2", "id005").toJson())
        assertThat(q.parameters.elementAt(4).toJson()).isEqualTo(SqlParameter("@age", 30).toJson())
    }

    @Test
    fun `buildQuerySpec for count should get correct SQL`() {
        val q = buildQuerySpec(
            filter = json {
                "fullName.last" to "Hanks"
                "id" to listOf("id001", "id002", "id005")
                "age" to 30
            },
            sort = json {
                "_ts" to "DESC"
            },
            offset = 10,
            limit = 20,
            count = true
        )

        assertThat(q.queryText.trim()).isEqualTo("""SELECT COUNT(1) FROM c WHERE (c.fullName.last = @fullName__last) AND (c.id IN (@id__0, @id__1, @id__2)) AND (c.age = @age)""")

        assertThat(q.parameters.elementAt(0).toJson()).isEqualTo(SqlParameter("@fullName__last", "Hanks").toJson())
        assertThat(q.parameters.elementAt(1).toJson()).isEqualTo(SqlParameter("@id__0", "id001").toJson())
        assertThat(q.parameters.elementAt(2).toJson()).isEqualTo(SqlParameter("@id__1", "id002").toJson())
        assertThat(q.parameters.elementAt(3).toJson()).isEqualTo(SqlParameter("@id__2", "id005").toJson())
        assertThat(q.parameters.elementAt(4).toJson()).isEqualTo(SqlParameter("@age", 30).toJson())
    }

    @Test
    fun `Find should work with filter`() {

        val user1 = User("rai_find_filter", "id_find_filter1", FullName("Elise", "Hanks"))
        val user2 = User("rai_find_filter", "id_find_filter2", FullName("Matt", "Hanks"))
        val user3 = User("rai_find_filter", "id_find_filter3", FullName("Tom", "Henry"))

        try {
            // prepare
            db.upsert(collection = "Data", data = user1, partition = "Users")
            db.upsert(collection = "Data", data = user2, partition = "Users")
            db.upsert(collection = "Data", data = user3, partition = "Users")

            // test find
            var users = db.find(
                "Data",
                filter = json {
                    "fullName.last" to "Hanks"
                    "fullName.first" to "Elise"
                },
                sort = json { "id" to "ASC" },
                offset = 0,
                limit = 10,
                partition = "Users"
            ).toList<User>()

            assertThat(users.size).isEqualTo(1)
            assertThat(users[0]).isEqualTo(user1)

            // test find 2
            users = db.find(
                "Data",
                filter = json {
                    "fullName.last" to "Hanks"
                    "id" to listOf(user1.id, user2.id, user3.id)
                },
                sort = json {
                    "_ts" to "DESC"
                },
                partition = "Users"
            ).toList<User>()

            assertThat(users.size).isEqualTo(2)
            assertThat(users[0]).isEqualTo(user2)

            // test find 3
            users = db.find(
                "Data",
                limit = 2,
                partition = "Users"
            ).toList<User>()

            assertThat(users.size).isEqualTo(2)
            assertThat(users[0]).isEqualTo(user3)
        } finally {
            db.delete("Data", user1.id, "Users")
            db.delete("Data", user2.id, "Users")
            db.delete("Data", user3.id, "Users")
        }
    }

    @Test
    fun `Count should work with filter`() {

        val user1 = User("rai_count_filter", "id_count_filter1", FullName("Tom", "Luke"))
        val user2 = User("rai_counnt_filter", "id_count_filter2", FullName("Betty", "Luke"))

        try {
            // prepare
            db.upsert(collection = "Data", data = user1, partition = "Users")
            db.upsert(collection = "Data", data = user2, partition = "Users")

            // test find count
            var count = db.count(
                "Data",
                filter = json {
                    "fullName.last" to "Luke"
                },
                partition = "Users"
            )

            assertThat(count).isEqualTo(2)
        } finally {
            db.delete("Data", user1.id, "Users")
            db.delete("Data", user2.id, "Users")
        }
    }
}
