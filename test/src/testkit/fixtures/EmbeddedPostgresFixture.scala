package testkit.fixtures

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import munit.FunSuite

import java.sql.Connection
import scala.compiletime.uninitialized

trait EmbeddedPostgresFixture extends FunSuite {

  var server: EmbeddedPostgres = uninitialized

  override def beforeEach(context: BeforeEach): Unit = {
    server = EmbeddedPostgres.builder().setPort(5432).start()
  }

  override def afterEach(context: AfterEach): Unit = {
    server.close()
    server = null
  }

  val connection = FunFixture[Connection](
    setup = { _ =>
      val pgDataSource = new org.postgresql.ds.PGSimpleDataSource()
      pgDataSource.setUser("postgres")
      pgDataSource.getConnection
    },
    teardown = { connection =>
      connection.close()
    }
  )

}
