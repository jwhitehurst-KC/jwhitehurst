library(apde.etl)
library(Microsoft365R)

# Sharepoint auth
AzureAuth::get_azure_token(
  resource = "https://storage.azure.com", 
  tenant = keyring::key_get("adl_tenant", "dev"),
  app = keyring::key_get("adl_app", "dev"),
  auth_type = "authorization_code",
  use_cache = F
)

# List of sharepoint lists
list_names <- c(
#  "BCCHP_Activities",
#  "BCCHP_Agencies",
#  "BCCHP_AgencyProcedure",
#  "BCCHP_AgencyService",
#  "BCCHP_Organizations",
  "BCCHP_People"#,
#  "BCCHP_PersonActivity",
#  "BCCHP_Procedures",
#  "BCCHP_Programs",
#  "BCCHP_Screenings",
#  "BCCHP_Services",
#  "BCCHP_ZipsCounties"
)

# set Sharepoint site
spsite <- get_sharepoint_site(site_url = "https://kc1.sharepoint.com/teams/DPH-BCCHP")

for(list_name in list_names) {
  message(paste0("Begin working on list: ", list_name))
  # get current list on sharepoint
  splist <- spsite$get_list(list_name)
  # fetch all items from list and delete
  items <- splist$list_items(as_data_frame = FALSE)
  if (length(items) > 0) {
    message(paste0("...Deleting ", length(items), " items from list."))
    lapply(items, function(item) {
      item$delete(confirm = FALSE)
    })
    message("...All items successfully deleted.")
  } else {
    message("...The list is already empty.")
  }
  # get sharepoint list fields
  list_fields <- splist$get_column_info()
  # load data from csv
  df <- read.csv(tolower(stringr::str_replace(paste0("c:/temp/bcchp/", list_name, ".csv"), "BCCHP_", "")), stringsAsFactors = FALSE)
  for(i in 1:length(names(df))) {
    if(!is.logical(df[,i])) {
      df[,i] <- as.character(df[,i])
    } else if(!is.na(list_fields[which(list_fields$displayName == names(df)[i])[[1]], "text"]["maxLength"])) {
      df[,i] <- as.character(df[,i])
    }
  }
  # rename columns to match the names sharepoint has set
  names(df) <- setNames(list_fields$name, list_fields$displayName)[names(df)]
  names(df) <- stringr::str_replace(names(df), "LinkTitle", "Title")
  df$Title <- as.character(df$Title)
  # load data to sharepoint
  message(paste0("...Loading ", nrow(df), " rows of data to list."))
  last_batch <- nrow(df) %% 10
  batches <- (nrow(df) - last_batch) / 10
  for(i in 1:(batches + 1)) {
    if(i <= batches) {
      batch_end <- 10 * i
      batch_start <- batch_end - 9
    } else {
      batch_end <- nrow(df)
      batch_start <- nrow(df) - (last_batch - 1)
    }
    batch <- df[batch_start:batch_end, ]
    message(paste0("......Loading rows ", batch_start, " through ", batch_end, "."))
    splist$bulk_import(batch)
  }
  message("...Data appended to list.")
}

