package piggy

import fastparse.*
import NoWhitespace.*
import piggy.Sql.{PreparedStatementArg, PsArgHolder}

/** Parser for SQL with named parameters (e.g., :paramName). Converts named
  * parameters to positional JDBC placeholders (?).
  */
object NamedParameterParser {

  /** Represents a parsed SQL fragment */
  private enum SqlFragment {
    case Literal(text: String)    // Plain SQL text or string literal
    case NamedParam(name: String) // Named parameter like :name
  }

  /** Parse a SQL string literal with single quotes. Handles escaped quotes
    * using SQL's doubled quote syntax (' '). Matches: 'anything with
    * single-escaped quotes'
    */
  private def singleQuotedString[$: P]: P[SqlFragment] =
    P("'" ~ (("''" | CharPred(_ != '\'')).rep).! ~ "'")
      .map(text => SqlFragment.Literal("'" + text + "'"))

  /** Parse a SQL string literal with double quotes. Handles escaped quotes
    * using SQL's doubled quote syntax (""). Matches: "anything with
    * double-escaped quotes"
    */
  private def doubleQuotedString[$: P]: P[SqlFragment] =
    P("\"" ~ (("\"\"" | CharPred(_ != '"')).rep).! ~ "\"")
      .map(text => SqlFragment.Literal("\"" + text + "\""))

  /** Parse a string literal (single or double-quoted) */
  private def stringLiteral[$: P]: P[SqlFragment] =
    P(singleQuotedString | doubleQuotedString)

  /** Parse a named parameter like :paramName */
  private def namedParameter[$: P]: P[SqlFragment] =
    P(":" ~ CharIn("a-zA-Z_") ~ CharIn("a-zA-Z0-9_").rep).!.map(s =>
      SqlFragment.NamedParam(s.drop(1))
    ) // drop the leading ':'

  /** Parse any SQL text that's not a string literal or named parameter */
  private def sqlText[$: P]: P[SqlFragment] =
    P(CharsWhile(c => c != ':' && c != '\'' && c != '"', 1).!)
      .map(text => SqlFragment.Literal(text))

  /** Parse a standalone colon (not part of a named parameter) */
  private def standaloneColon[$: P]: P[SqlFragment] =
    P(":").map(_ => SqlFragment.Literal(":"))

  /** Parse a single SQL fragment */
  private def sqlFragment[$: P]: P[SqlFragment] =
    P(stringLiteral | namedParameter | standaloneColon | sqlText)

  /** Parse complete SQL into fragments */
  private def sqlParser[$: P]: P[Seq[SqlFragment]] =
    P(sqlFragment.rep ~ End)

  /** Parse SQL with named parameters, returning (SQL with ?, parameter names in
    * order). Handles string literals to avoid false positives.
    * @param sql
    *   SQL string with :paramName placeholders
    * @return
    *   Tuple of (SQL with ?, Seq of parameter names in order)
    */
  def parse(sql: String): (String, Seq[String]) = {
    fastparse.parse(sql, sqlParser(using _)) match {
      case Parsed.Success(fragments, _) =>
        val sqlBuilder = new StringBuilder
        val paramNames = scala.collection.mutable.ListBuffer[String]()

        fragments.foreach {
          case SqlFragment.Literal(text)    =>
            sqlBuilder.append(text)
          case SqlFragment.NamedParam(name) =>
            sqlBuilder.append('?')
            paramNames += name
        }

        (sqlBuilder.toString, paramNames.toSeq)
      case f: Parsed.Failure            =>
        throw new Exception(
          s"Failed to parse SQL: ${f.trace().longAggregateMsg}"
        )
    }
  }

  /** Convert SQL with named parameters to a PsArgHolder with positional
    * parameters.
    * @param sqlTemplate
    *   SQL string with :paramName placeholders
    * @param namedParams
    *   Map of parameter names to values
    * @return
    *   PsArgHolder with positional parameters
    */
  def toPsArgHolder(
      sqlTemplate: String,
      namedParams: Map[String, PreparedStatementArg]
  ): PsArgHolder = {
    val (sql, paramNames) = parse(sqlTemplate)

    // Validate all parameters are provided
    val missing = paramNames.toSet.diff(namedParams.keySet)
    if (missing.nonEmpty) {
      throw PiggyException.MissingNamedParametersException(
        sqlTemplate,
        missing.toSeq
      )
    }

    // Warn about unused parameters (could be removed in production)
    val extra = namedParams.keySet.diff(paramNames.toSet)
    if (extra.nonEmpty) {
      System.err.println(
        s"Warning: Unused named parameters: ${extra.mkString(", ")}"
      )
    }

    // Map named parameters to positional order
    val positionalArgs = paramNames.map { name =>
      namedParams(name) // Safe because we validated above
    }

    PsArgHolder(sql, positionalArgs*)
  }
}
