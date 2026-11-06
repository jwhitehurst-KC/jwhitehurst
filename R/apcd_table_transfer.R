library(DBI)
library(glue)
library(apde.connect)

log_msg <- function(msg) {
    ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    message(sprintf("[%s] %s", ts, msg))
  }

schema <- "stg_claims"
tables <- c(
  "apcd_medical_claim",
  "apcd_medical_claim_diagnosis", 
  "apcd_medical_claim_header")

for(table in tables) {
  log_msg(glue::glue("Begin transferring data to [{schema}].[{table}]"))
  conn <- apde.connect::connect(database = "inthealth_edw")
  table_tmp <- paste0(table, "_tmp")
  y <- DBI::dbGetQuery(
    conn,
    glue::glue_sql(
      "SELECT DISTINCT 
        YEAR(first_service_dt) AS y
      FROM {`schema`}.{`table_tmp`}
      ORDER BY 
        YEAR(first_service_dt)",
        .con = conn
    )
  )
  DBI::dbDisconnect(conn)
  for(i in 1:nrow(y)) {
    sy <- y[i, "y"]
    for(sm in 1:12) {
      if(sm == 12) {
        em <- 1
        ey <- sy + 1
      } else {
        em <- sm + 1
        ey <- sy
      }
      log_msg(glue::glue("[{schema}].[{table}] - YEAR: {sy} - MONTH {sm}"))
      conn <- apde.connect::connect(database = "inthealth_edw")
      sdate <- paste0(sy, "-", sm, "-01")
      edate <- paste0(ey, "-", em, "-01")
      DBI::dbExecute(
        conn,
        glue::glue_sql(
          "INSERT INTO {`schema`}.{`table`}
          SELECT 
            * 
          FROM {`schema`}.{`table_tmp`}
          WHERE
            first_service_dt >= {sdate}
            AND first_service_dt < {edate}
          ",
          .con = conn
        )
      )
      DBI::dbDisconnect(conn)
    }
  }
  log_msg(glue::glue("Transfer to [{schema}].[{table}] COMPLETE"))
}



