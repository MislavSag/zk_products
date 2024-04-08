library(httr)
library(data.table)
library(mongolite)
library(jsonlite)


# SET UP ------------------------------------------------------------------
# Terms to search for
terms = c("INA d.d.", "Ina - industrija nafte d.d.", "27759560625")

# Feilds from json we would like to keep
projection_list = list(
  `_id` = 0,
  `lrUnitId` = 1,
  `lrUnitNumber` = 1,
  `mainBookId` = 1,
  `mainBookName` = 1,
  `cadastreMunicipalityId` = 1,
  institutionName = 1
)
projection_json = toJSON(projection_list, auto_unbox = TRUE)


# DATA API SEARCH ---------------------------------------------------------
# DATA API search loop
zk_l = lapply(terms, function(x) {
  p = GET("http://dac.hr/api/v1/query", 
          query = list(
            q = x,
            history = "true",
            limit = 20000,
            skip = 0
          ), 
          add_headers(`X-DataApi-Key` = Sys.getenv("TOKEN")))
  res = content(p)
  res = rbindlist(res$hits)
  as.data.table(cbind.data.frame(term = x, res))
})
lapply(zk_l, function(x) nrow(x))
zkdt = rbindlist(zk_l)
zkdt = unique(zkdt)

# Specific filtering
# 1) Trezor invest
zkdt = zkdt[type == "zk"]


# MONGO SEARCH ------------------------------------------------------------
# Exact search by OIB
oibs = c("27759560625", "15538072333")
con = mongo(collection = "zkjson", url = Sys.getenv("MONGO_URL"))
mongo_oib = lapply(oibs, function(o) {
  q = sprintf('{"ownershipSheetB.lrUnitShares.lrOwners.taxNumber": "%s"}', o)
  result = con$find(q, fields = projection_json)
  as.data.table(cbind.data.frame(term = o, result))
})
con$disconnect()
mongo_oib = rbindlist(mongo_oib, fill = TRUE)
mongo_oib = mongo_oib[, lapply(.SD, unlist)]
mongo_oib[, unique(institutionName)]

# Standard Atlas search
pipeline_list = list(
  list(
    "$search" = list(
      "index" = "standard",
      "text" = list(
        "query" = terms,
        "path" = c("ownershipSheetB.lrUnitShares.lrOwners.name", 
                   "ownershipSheetB.lrUnitShares.ubSharesAndEntries.lrOwners.name")
      )
    )
  ),
  list(
    "$limit" = 50000
  )
)
pipeline_json = toJSON(pipeline_list, auto_unbox = TRUE)
con = mongo(collection = "zkjson", url = Sys.getenv("MONGO_URL"))
result = con$aggregate(pipeline_json)
con$disconnect()


# ANALYSE RESULTS ---------------------------------------------------------
# 
dim(result)



# Save
# fwrite(zkdt, "INA_podaci.csv")