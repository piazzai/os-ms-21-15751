# load libraries

library(car)
library(dplyr)
library(data.table)
library(stringr)
library(xml2)
library(reshape2)
library(doParallel)
library(jsonlite)
library(Hmisc)
library(rvest)
library(RSelenium)
library(stringi)
library(fastDummies)
library(stringdist)
library(survival)
library(ggplot2)
library(lmtest)
library(anytime)
library(lsa)
library(binman)
library(cowplot)
library(ineq)
library(LaplacesDemon)
library(abind)

# set number of cores for parallel processing (default: half of system cores)

clust <- ceiling(detectCores() / 2)

# set paths to local files (input your paths)

paths <-
  list(
    discogs = "path/to/discogs/",
    musicbrainz = "path/to/mbdump/",
    listenbrainz = "path/to/listens/",
    extra = "path/to/extra/"
  )

# set discogs login details (input your details)

login <- list(user = "username", pass = "password")

# find singles in musicbrainz dump

relgroup_types <-
  read.csv(
    paste0(paths$musicbrainz, "release_group_primary_type"),
    sep = "\t",
    as.is = T,
    header = F
  ) %>% select("type_key" = V1, "type" = V2)

relgroups <-
  read.csv(
    paste0(paths$musicbrainz, "release_group"),
    sep = "\t",
    as.is = T,
    header = F
  ) %>% select("mbid" = V2,
               "title" = V3,
               "type" = V5)

single_type_key <- subset(relgroup_types, type == "Single")$type_key

singles <-
  subset(relgroups, type == single_type_key) %>%
  select(-c("type")) %>%
  data.table()

# retrieve discogs master id from musicbrainz

xref_discogs <- function(x) {
  ns <-
    paste("https://musicbrainz.org/release-group", x, sep = "/") %>%
    read_html()
  bdis <- xml_find_all(ns, ".//bdi") %>%
    as.character()
  xref <- bdis[grepl("discogs.com", bdis)] %>%
    str_extract("/[[:digit:]]*?<") %>%
    str_remove_all(paste("/", "<", sep = "|"))
  if (length(xref) > 0) {
    xref
  } else {
    NA
  }
}

singles$master <-
  mcmapply(xref_discogs, singles$mbid, mc.cores = clust)

# keep only singles with unique discogs reference

master_clean <- as.character(singles$master)

master_clean[master_clean == "NA"] <- NA
master_clean[which(grepl(",", master_clean))] <- NA

singles$master <- master_clean

singles <- singles[!is.na(master)]

# function to extract data from raw xml files

clean_xml <- function (ns, x, y = NULL) {
  if (is.null(y)) {
    y1 <- paste("<", x, ">", sep = "")
    y2 <- paste("</", x, ">", sep = "")
  } else {
    y1 <- paste("<", y, ">", sep = "")
    y2 <- paste("</", y, ">", sep = "")
  }
  ns <- xml_find_first(ns, paste(".//", x, sep = "")) %>%
    as.character()
  mat <-
    str_extract_all(ns, paste(y1, y2, sep = "(.*?)"), simplify = T)
  apply(mat, 1, paste, collapse = "_") %>%
    str_remove_all(paste(y1, y2, sep = "|")) %>%
    str_remove("_*$")
}

# retrieve release year from discogs dump

masters <- paste0(paths$discogs, "discogs_20200401_masters.xml") %>%
  read_xml() %>%
  xml_children()

masters_id <- as.character(xml_attrs(masters))
masters_year <- as.integer(clean_xml(masters, "year"))
masters_title <- clean_xml(masters, "title")
masters_mainrel <- as.integer(clean_xml(masters, "main_release"))

masters <-
  data.table(
    "master" = masters_id,
    "year" = masters_year,
    "title" = masters_title,
    "mainrel" = masters_mainrel
  )

singles <-
  left_join(singles,
            masters,
            by = "master",
            suffix = c("_mb", "_discogs")) %>%
  data.table()

singles[year == 0]$year <- NA

singles <- singles[!is.na(year)]

# retrieve tracklists and features from acousticbrainz

track_mb <- function(x) {
  ns <-
    paste("https://musicbrainz.org/release-group", x, sep = "/") %>%
    read_html()
  tds <- xml_find_all(ns, ".//td") %>%
    as.character()
  release_ids <- tds[grepl("href=\"/release/", tds)] %>%
    str_extract("release/.*?\"") %>%
    str_remove_all(paste("release/", "\"", sep = "|"))
  all_ids <- as.character()
  for (id in release_ids) {
    ns <- paste0("https://musicbrainz.org/release/", id) %>%
      read_html()
    tds <- xml_find_all(ns, ".//td") %>%
      as.character()
    this_ids <- tds[grepl("href=\"/recording", tds)] %>%
      str_extract("recording/.*?\"") %>%
      str_remove_all(paste("recording/", "\"", sep = "|"))
    all_ids <- c(all_ids, this_ids)
  }
  unique(all_ids)
}

registerDoParallel(clust)
tracks <- foreach(i = singles$mbid, .combine = "rbind") %dopar% {
  track_mbids <- track_mb(i)
  data.table("mbid" = i, "track_mbid" = track_mbids)
}
stopImplicitCluster()

tracks <- tracks[!is.na(track_mbid)]

track_25 <-
  split(tracks$track_mbid, as.integer((seq_along(tracks$track_mbid) - 1) / 25))

registerDoParallel(clust)
track_feats <-
  foreach(i = track_25, .combine = "rbind") %dopar% {
    ids <- unlist(i) %>%
      as.character() %>%
      paste(collapse = ":0;")
    json <-
      paste("https://acousticbrainz.org/api/v1/low-level?recording_ids",
            ids,
            sep = "=") %>%
      read_json()
    Sys.sleep(1)
    found <- attributes(json) %>%
      unlist() %>%
      as.character()
    all_feats <- data.table(
      "track_mbid" = as.character(),
      "track_title" = as.character(),
      "length" = as.character(),
      "key_key" = as.character(),
      "key_scale" = as.character(),
      "key_strength" = as.character(),
      "chords_key" = as.character(),
      "chords_scale" = as.character(),
      "danceability" = as.character(),
      "bpm" = as.character(),
      "beats_count" = as.character(),
      "average_loudness" = as.character(),
      "dynamic_complexity" = as.character(),
      "chords_changes_rate" = as.character(),
      "chords_number_rate" = as.character()
    )
    for (j in found) {
      this_json <- json[[j]]$`0`
      feats <-
        c(
          j,
          this_json$metadata$tags$title,
          this_json$metadata$audio_properties$length,
          this_json$tonal$key_key,
          this_json$tonal$key_scale,
          this_json$tonal$key_strength,
          this_json$tonal$chords_key,
          this_json$tonal$chords_scale,
          this_json$rhythm$danceability,
          this_json$rhythm$bpm,
          this_json$rhythm$beats_count,
          this_json$lowlevel$average_loudness,
          this_json$lowlevel$dynamic_complexity,
          this_json$tonal$chords_changes_rate,
          this_json$tonal$chords_number_rate
        )
      feats <- as.matrix(feats) %>%
        t() %>%
        data.table()
      if (ncol(feats) == ncol(all_feats)) {
        colnames(feats) <- colnames(all_feats)
        all_feats <- rbind(all_feats, feats)
      }
    }
    distinct(all_feats)
  }
stopImplicitCluster()

track_feats <- apply(track_feats, 2, unlist) %>%
  data.table()

tracks <- left_join(tracks, track_feats, "track_mbid") %>%
  data.table()

# scrape discogs revision histories (only the last revision)

remDr <-
  remoteDriver(remoteServerAddr = "localhost",
               port = 4445L,
               browserName = "chrome")

remDr$open(silent = T)

mainrels <-
  data.table(
    "mainrel" = as.character(),
    "rev" = as.integer(),
    "user" = as.character(),
    "stamp" = as.character(),
    "genre" = as.character(),
    "style" = as.character()
  )

for (x in singles$mainrel) {
  remDr$navigate(paste0("https://www.discogs.com/release/", x, "/history"))
  rnorm(1, 3, 2) %>%
    abs() %>%
    Sys.sleep()
  if (grepl("https://auth.discogs.com/login", remDr$getCurrentUrl()[[1]])) {
    username <- remDr$findElement(using = "id", value = "username")
    username$clearElement()
    username$sendKeysToElement(list(login@user))
    password <- remDr$findElement(using = "id", value = "password")
    password$clearElement()
    password$sendKeysToElement(list(login@pass, "\uE007"))
    rnorm(1, 3, 2) %>%
      abs() %>%
      Sys.sleep()
  }
  rev_history <- remDr$getPageSource()[[1]] %>%
    read_html()
  no_rev <- html_nodes(rev_history, "div.history") %>%
    html_nodes("tr.datarow") %>%
    length()
  for (i in 1:no_rev) {
    remDr$navigate(paste0("https://www.discogs.com/release/", x, "/history?rev=", i))
    this_rev <- remDr$getPageSource()[[1]] %>%
      read_html()
    user <- html_nodes(this_rev, "tr.datarow.highlight") %>%
      html_nodes("a.linked_username") %>%
      html_text() %>%
      str_remove_all(fixed("  ")) %>%
      str_remove_all("\n")
    user <- ifelse(length(user) > 0, user, NA)
    stamp <- html_nodes(this_rev, "tr.datarow.highlight") %>%
      html_nodes("span") %>%
      as.character()
    stamp <-
      stamp[grepl("[[:digit:]]*{2}-[[:alpha:]]*{3}-[[:digit:]]*{2}",
                  stamp)] %>%
      str_extract("[[:digit:]]*?-[[:alnum:]]*?-[[:digit:]]*? ") %>%
      str_remove(fixed(" "))
    stamp <- ifelse(length(stamp) > 0, stamp, NA)
    profile <- html_nodes(this_rev, "div.content") %>%
      html_text() %>%
      str_remove_all(fixed("  ")) %>%
      str_remove_all("\n")
    profile <- tail(profile, 2) %>%
      as.matrix() %>%
      t() %>%
      data.table()
    profile <- cbind(as.integer(x), i, user, stamp, profile)
    colnames(profile) <- colnames(mainrels)
    if (grepl("[[:alnum:]]", profile$style)) {
      break
    } else {
      rnorm(1, 3, 2) %>%
        abs() %>%
        Sys.sleep()
    }
  }
  mainrels <- rbind(mainrels, profile)
}

remDr$close()

mainrels <- distinct(mainrels)

# break down revision timestamps to d/m/y columns

correct_month <- function (x) {
  ifelse(x == "Jan", 1, ifelse(x == "Feb", 2, ifelse(
    x == "Mar", 3, ifelse(x == "Apr", 4, ifelse(
      x == "May", 5, ifelse(x == "Jun", 6, ifelse(
        x == "Jul", 7, ifelse(x == "Aug", 8, ifelse(
          x == "Sep", 9, ifelse(x == "Oct", 10, ifelse(x == "Nov", 11, ifelse(x == "Dec", 12, NA)))
        ))
      ))
    ))
  )))
}

mainrels$stamp_day <- substr(mainrels$stamp, 1, 2) %>%
  as.integer()

mainrels$stamp_month <-
  substr(mainrels$stamp, 4, 6) %>%
  correct_month()

mainrels$stamp_year <- substr(mainrels$stamp, 8, 9)

mainrels$stamp_year <-
  ifelse(is.na(mainrels$stamp_year),
         NA,
         paste0("20", mainrels$stamp_year)) %>%
  as.integer()

# correct some style names for consistency

mainrels$genre <-
  str_replace_all(mainrels$genre, "Folk, World, & Country", "Folk World & Country")

mainrels$style <-
  str_replace_all(mainrels$style, "Shoegazer", "Shoegaze")

mainrels$style <-
  str_replace_all(mainrels$style, "R&B/Swing", "RnB/Swing")

mainrels[nchar(genre) == 1]$genre <- NA
mainrels[nchar(style) == 1]$style <- NA

# retrieve style hierarchy from discogs edit form

remDr <-
  remoteDriver(remoteServerAddr = "localhost",
               port = 4445L,
               browserName = "chrome")

remDr$open(silent = T)

remDr$navigate(paste0("https://www.discogs.com/release/edit/", mainrels$mainrel[1]))

if (grepl("https://auth.discogs.com/login", remDr$getCurrentUrl()[[1]])) {
  username <- remDr$findElement(using = "id", value = "username")
  username$clearElement()
  username$sendKeysToElement(list(login@user))
  password <- remDr$findElement(using = "id", value = "password")
  password$clearElement()
  password$sendKeysToElement(list(login@pass, "\uE007"))
  rnorm(1, 3, 2) %>%
    abs() %>%
    Sys.sleep()
}

edit_page <- remDr$getPageSource()[[1]] %>%
  read_html()

scripts <- html_nodes(edit_page, "script")

tagging_script <- scripts[grepl("style_map", scripts)]

style_map <- as.character(tagging_script) %>%
  str_extract("\\[\\{\"styles\":[[:alnum:][:punct:][:blank:]]*?\\}\\]")

genres <-
  str_extract_all(style_map, "\"name\": \".*?\"") %>%
  unlist() %>%
  str_remove("\"name\": \"") %>%
  str_remove("\"$") %>%
  stri_unescape_unicode()

hierarchy <-
  str_extract_all(style_map, "\\[\"[[:punct:][:alnum:][:blank:]]*?\\]") %>%
  unlist() %>%
  str_extract_all("\".*?\"") %>%
  lapply(str_remove_all, "\"") %>%
  lapply(stri_unescape_unicode)

names(hierarchy) <- genres

remDr$close()

# dummify nominal acoustic features

clean_tracks <-
  dummy_cols(
    tracks[complete.cases(tracks)],
    select_columns = c("key_key", "key_scale", "chords_key", "chords_scale"),
    remove_first_dummy = T,
    remove_selected_columns = T
  )

clean_tracks <-
  cbind(clean_tracks[, 1:3], apply(clean_tracks[,-c(1:3)], 2, as.numeric))

# calculate average features of tracks within singles

clean_singles <- singles[mbid %in% clean_tracks$mbid]

registerDoParallel(clust)
singles_feat <-
  foreach(i = clean_singles$mbid, .combine = "rbind") %dopar% {
    feats <- clean_tracks[mbid == i,-c(1:3)] %>%
      colMeans()
    c(i, feats)
  }
stopImplicitCluster()

singles_feat <-
  data.table("mbid" = singles_feat[, 1], apply(singles_feat[,-1], 2, as.numeric))

clean_singles <-
  left_join(clean_singles, singles_feat, "mbid") %>%
  data.table()

# generate style-year and genre-year data tables

style_years <-
  data.table("style" = as.character(),
             "genre" = as.character(),
             "year" = as.integer())

for (i in sort(unique(mainrels$stamp_year))) {
  this_year <- data.table(
    "style" = hierarchy %>%
      unlist() %>%
      as.character(),
    "genre" = hierarchy %>%
      unlist() %>%
      names() %>%
      str_remove("[[:digit:]]*?$"),
    "year" = i
  )
  style_years <- rbind(style_years, this_year)
}

style_years$genre <-
  str_replace_all(style_years$genre,
                  "Folk, World, & Country",
                  "Folk World & Country")

genre_years <-
  data.table("genre" = as.character(), "year" = as.integer())

for (i in sort(unique(mainrels$stamp_year))) {
  this_year <- data.table("genre" = names(hierarchy),
                          "year" = i)
  genre_years <- rbind(genre_years, this_year)
}

genre_years$genre <-
  str_replace_all(genre_years$genre,
                  "Folk, World, & Country",
                  "Folk World & Country")

# counts of genre and style tags up until the previous year

count_style_tags <- function (s, g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  mainrels[grepl(paste(
    paste("^", s, ",", sep = ""),
    paste(", ", s, ",", sep = ""),
    paste(", ", s, "$", sep = ""),
    paste("^", s, "$", sep = ""),
    sep = "|"
  ), style) & stamp_year == y & grepl(g, genre)] %>%
    nrow()
}

count_genre_tags <- function (g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  mainrels[stamp_year == y & grepl(g, genre)] %>%
    nrow()
}

style_years$style_tags <-
  mcmapply(
    count_style_tags,
    style_years$style,
    style_years$genre,
    style_years$year,
    mc.cores = clust
  )

style_years$genre_tags <-
  mcmapply(count_genre_tags,
           style_years$genre,
           style_years$year,
           mc.cores = clust)

style_years$sibling_tags <-
  style_years$genre_tags - style_years$style_tags

genre_years$genre_tags <-
  mcmapply(count_genre_tags,
           genre_years$genre,
           genre_years$year,
           mc.cores = clust)

# compute the specificity of styles

feats <- colnames(clean_singles)[c(16:27, 8, 28:39, 9:11, 7)]

compute_informative_style <- function (s, g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  style_mainrels <-
    mainrels[grepl(paste(
      paste("^", s, ",", sep = ""),
      paste(", ", s, ",", sep = ""),
      paste(", ", s, "$", sep = ""),
      paste("^", s, "$", sep = ""),
      sep = "|"
    ), style) & stamp_year == y & grepl(g, genre)]$mainrel
  style_feats <-
    clean_singles[mainrel %in% style_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(style_feats) == 0) {
    return(NA)
  } else {
    genre_mainrels <-
      mainrels[stamp_year == y & grepl(g, genre)]$mainrel
    genre_feats <-
      clean_singles[mainrel %in% genre_mainrels, ..feats] %>%
      as.matrix()
    if (nrow(genre_feats) < 2) {
      return(NA)
    } else {
      covmat <- cov(genre_feats) + diag(ncol(genre_feats)) * .001
      distances <-
        foreach(i = 1:nrow(style_feats), .combine = "c") %do% {
          mahalanobis(style_feats[i,], colMeans(genre_feats), covmat) %>%
            sqrt()
        }
      mean(distances)
    }
  }
}

style_years$informative <-
  mcmapply(
    compute_informative_style,
    style_years$style,
    style_years$genre,
    style_years$year,
    mc.cores = clust
  )

# compute the specificity of genres

compute_informative_genre <- function (g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  genre_mainrels <-
    mainrels[stamp_year == y & grepl(g, genre)]$mainrel
  genre_feats <-
    clean_singles[mainrel %in% genre_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(genre_feats) == 0) {
    return(NA)
  } else {
    all_mainrels <-
      mainrels[stamp_year == y]$mainrel
    all_feats <-
      clean_singles[mainrel %in% all_mainrels, ..feats] %>%
      as.matrix()
    if (nrow(all_feats) < 2) {
      return(NA)
    } else {
      covmat <- cov(all_feats) + diag(ncol(all_feats)) * .001
      distances <-
        foreach(i = 1:nrow(genre_feats), .combine = "c") %do% {
          mahalanobis(genre_feats[i,], colMeans(all_feats), covmat) %>%
            sqrt()
        }
      mean(distances)
    }
  }
}

genre_years$informative <-
  mcmapply(compute_informative_genre,
           genre_years$genre,
           genre_years$year,
           mc.cores = clust)

# compute the distinctiveness of styles

compute_distinctive_style <- function (s, g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  style_mainrels <- mainrels[grepl(paste(
    paste("^", s, ",", sep = ""),
    paste(", ", s, ",", sep = ""),
    paste(", ", s, "$", sep = ""),
    paste("^", s, "$", sep = ""),
    sep = "|"
  ), style) & stamp_year == y & grepl(g, genre)]$mainrel
  style_feats <-
    clean_singles[mainrel %in% style_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(style_feats) == 0) {
    return(NA)
  } else {
    distances <- as.numeric()
    siblings <- setdiff(hierarchy[[g]], s)
    for (j in siblings) {
      sibling_mainrels <- mainrels[grepl(paste(
        paste("^", j, ",", sep = ""),
        paste(", ", j, ",", sep = ""),
        paste(", ", j, "$", sep = ""),
        paste("^", j, "$", sep = ""),
        sep = "|"
      ), style) & stamp_year == y & grepl(g, genre)]$mainrel
      sibling_feats <-
        clean_singles[mainrel %in% sibling_mainrels, ..feats] %>%
        as.matrix()
      if (nrow(sibling_feats) < 2) {
        distances <- c(distances, NA)
      } else {
        covmat <- cov(sibling_feats) + diag(ncol(sibling_feats)) * .001
        sibling_distances <-
          foreach(i = 1:nrow(style_feats), .combine = "c") %do% {
            mahalanobis(style_feats[i,], colMeans(sibling_feats), covmat) %>%
              sqrt()
          }
        distances <- c(distances, mean(sibling_distances))
      }
    }
    if (length(na.omit(distances)) == 0) {
      return(NA)
    } else {
      mean(distances, na.rm = T)
    }
  }
}

style_years$distinctive <-
  mcmapply(
    compute_distinctive_style,
    style_years$style,
    style_years$genre,
    style_years$year,
    mc.cores = clust
  )

# compute the distinctiveness of genres

compute_distinctive_genre <- function (g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  genre_mainrels <-
    mainrels[stamp_year == y & grepl(g, genre)]$mainrel
  genre_feats <-
    clean_singles[mainrel %in% genre_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(genre_feats) == 0) {
    return(NA)
  } else {
    distances <- as.numeric()
    siblings <- setdiff(names(hierarchy), g)
    for (j in siblings) {
      sibling_mainrels <-
        mainrels[stamp_year == y & grepl(j, genre)]$mainrel
      sibling_feats <-
        clean_singles[mainrel %in% sibling_mainrels, ..feats] %>%
        as.matrix()
      if (nrow(sibling_feats) < 2) {
        distances <- c(distances, NA)
      } else {
        covmat <- cov(sibling_feats) + diag(ncol(sibling_feats)) * .001
        sibling_distances <-
          foreach(i = 1:nrow(genre_feats), .combine = "c") %do% {
            mahalanobis(genre_feats[i,], colMeans(sibling_feats), covmat) %>%
              sqrt()
          }
        distances <- c(distances, mean(sibling_distances))
      }
    }
    if (length(na.omit(distances)) == 0) {
      return(NA)
    } else {
      mean(distances, na.rm = T)
    }
  }
}

genre_years$distinctive <-
  mcmapply(compute_distinctive_genre,
           genre_years$genre,
           genre_years$year,
           mc.cores = clust)

# generate single-style and single-genre data tables

registerDoParallel(clust)
single_styles <-
  foreach(i = clean_singles$mbid, .combine = "rbind") %dopar% {
    data.table(
      "mbid" = i,
      "style" = hierarchy %>%
        unlist() %>%
        as.character(),
      "genre" = hierarchy %>%
        unlist() %>%
        names() %>%
        str_remove("[[:digit:]]*?$")
    )
  }
stopImplicitCluster

single_styles$genre <-
  str_replace_all(single_styles$genre,
                  "Folk, World, & Country",
                  "Folk World & Country")

singles <- left_join(singles, mainrels, "mainrel") %>%
  data.table()

single_styles <- eft_join(single_styles, singles, "mbid") %>%
  data.table()

registerDoParallel(clust)
single_genres <-
  foreach(i = clean_singles$mbid, .combine = "rbind") %dopar% {
    data.table("mbid" = i,
               "genre" = names(hierarchy))
  }
stopImplicitCluster()

single_genres$genre <-
  str_replace_all(single_genres$genre,
                  "Folk, World, & Country",
                  "Folk World & Country")

single_genres <- left_join(single_genres, singles, "mbid") %>%
  data.table()

# calculate dependent variables

compute_tagged_style <- function (s1, s2, g1, g2) {
  if (is.na(s2) | is.na(g2)) {
    NA
  } else {
    ifelse(grepl(paste(
      paste("^", g1, ",", sep = ""),
      paste(", ", g1, ",", sep = ""),
      paste(", ", g1, "$", sep = ""),
      paste("^", g1, "$", sep = ""),
      sep = "|"
    ), g2) &
      grepl(paste(
        paste("^", s1, ",", sep = ""),
        paste(", ", s1, ",", sep = ""),
        paste(", ", s1, "$", sep = ""),
        paste("^", s1, "$", sep = ""),
        sep = "|"
      ), s2), 1, 0)
  }
}

single_styles$tagged <-
  mcmapply(
    compute_tagged_style,
    single_styles$style.x,
    single_styles$style.y,
    single_styles$genre.x,
    single_styles$genre.y,
    mc.cores = clust
  )

single_styles <-
  select(
    single_styles,
    "single" = mbid,
    "style" = style.x,
    "genre" = genre.x,
    "rel_year" = year,
    "year" = stamp_year,
    user,
    tagged
  )

compute_tagged_genre <- function (g1, g2) {
  if (is.na(g2)) {
    NA
  } else {
    ifelse(grepl(g1, g2), 1, 0)
  }
}

single_genres$tagged <-
  mcmapply(compute_tagged_genre,
           single_genres$genre.x,
           single_genres$genre.y,
           mc.cores = clust)

single_genres <-
  select(
    single_genres,
    "single" = mbid,
    "genre" = genre.x,
    "rel_year" = year,
    "year" = stamp_year,
    user,
    tagged
  )

# join values of specificity and distinctiveness

single_styles <-
  left_join(single_styles, style_years, c("style", "genre", "year")) %>%
  data.table() %>% rename("tag_year" = year)

single_styles <- single_styles[complete.cases(single_styles)]

single_genres <-
  left_join(single_genres, genre_years, c("genre", "year")) %>%
  data.table() %>%
  rename("tag_year" = year)

single_genres <- single_genres[complete.cases(single_genres)]

# compute atypicality of singles with respect to styles

compute_atypicality_style <- function (x, s, g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  single_feats <- clean_singles[mbid == x, ..feats] %>%
    as.matrix()
  style_mainrels <- mainrels[grepl(paste(
    paste("^", s, ",", sep = ""),
    paste(", ", s, ",", sep = ""),
    paste(", ", s, "$", sep = ""),
    paste("^", s, "$", sep = ""),
    sep = "|"
  ), style) & stamp_year == y & grepl(g, genre)]$mainrel
  style_feats <-
    clean_singles[mainrel %in% style_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(style_feats) < 2) {
    return(NA)
  } else {
    covmat <- cov(style_feats) + diag(ncol(style_feats)) * .001
    mahalanobis(single_feats, colMeans(style_feats), covmat) %>%
      sqrt()
  }
}

single_styles$atypicality <-
  mcmapply(
    compute_atypicality_style,
    single_styles$single,
    single_styles$style,
    single_styles$genre,
    single_styles$tag_year,
    mc.cores = clust
  )

single_styles <- single_styles[complete.cases(single_styles)]

# compute atypicality of singles with respect to genres

compute_atypicality_genre <- function (x, g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  single_feats <- clean_singles[mbid == x, ..feats] %>%
    as.matrix()
  genre_mainrels <-
    mainrels[stamp_year == y & grepl(g, genre)]$mainrel
  genre_feats <-
    clean_singles[mainrel %in% genre_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(genre_feats) < 2) {
    return(NA)
  } else {
    covmat <- cov(genre_feats) + diag(ncol(genre_feats)) * .001
    mahalanobis(single_feats, colMeans(genre_feats), covmat) %>%
      sqrt()
  }
}

single_genres$atypicality <-
  mcmapply(
    compute_atypicality_genre,
    single_genres$single,
    single_genres$genre,
    single_genres$tag_year,
    mc.cores = clust
  )

single_genres <- single_genres[complete.cases(single_genres)]

# compute claims to styles and genres within single titles

single_styles <-
  left_join(single_styles,
            select(singles, "single" = mbid, "title" = title_discogs),
            "single") %>%
  data.table()

compute_selfclaim <- function (x, y, alg) {
  stringdist(tolower(x), tolower(y), alg, q = 2, p = .1)
}

single_styles$selfclaim_lv <-
  mcmapply(compute_selfclaim,
           single_styles$style,
           single_styles$title,
           "lv",
           mc.cores = clust)

single_styles$selfclaim_osa <-
  mcmapply(compute_selfclaim,
           single_styles$style,
           single_styles$title,
           "osa",
           mc.cores = clust)

single_styles$selfclaim_cos <-
  mcmapply(compute_selfclaim,
           single_styles$style,
           single_styles$title,
           "cosine",
           mc.cores = clust)

single_styles$selfclaim_jw <-
  mcmapply(compute_selfclaim,
           single_styles$style,
           single_styles$title,
           "jw",
           mc.cores = clust)

single_styles <- single_styles[,-c("title")]

single_genres <-
  left_join(single_genres,
            select(singles, "single" = mbid, "title" = title_discogs),
            "single") %>%
  data.table()

single_genres$selfclaim_lv <-
  mcmapply(compute_selfclaim,
           single_genres$genre,
           single_genres$title,
           "lv",
           mc.cores = clust)

single_genres$selfclaim_osa <-
  mcmapply(compute_selfclaim,
           single_genres$genre,
           single_genres$title,
           "osa",
           mc.cores = clust)

single_genres$selfclaim_cos <-
  mcmapply(compute_selfclaim,
           single_genres$genre,
           single_genres$title,
           "cosine",
           mc.cores = clust)

single_genres$selfclaim_jw <-
  mcmapply(compute_selfclaim,
           single_genres$genre,
           single_genres$title,
           "jw",
           mc.cores = clust)

single_genres <- single_genres[,-c("title")]

# join total number of tags

single_styles <-
  left_join(single_styles,
            rename(singles[, c("mbid", "style")], single = mbid, notags = style),
            "single") %>%
  data.table()

single_styles$notags <- str_count(single_styles$notags, ",") + 1

single_genres <-
  left_join(single_genres,
            rename(singles[, c("mbid", "genre")], single = mbid, notags = genre),
            "single") %>%
  data.table()

single_genres$notags <- str_count(single_genres$notags, ",") + 1

# load submission timestamps and compute delays

ab_stamps <- paste0(paths$extra, "abstamps.csv") %>%
  read.csv(as.is = T) %>%
  data.table()

ab_stamps <- ab_stamps[submission_offset == 0]

ab_stamps <- ab_stamps[gid %in% clean_tracks$track_mbid]

ab_stamps$submit_year <- substr(ab_stamps$submitted, 1, 4) %>%
  as.integer()

ab_stamps <- select(ab_stamps, gid, submit_year) %>%
  rename(track_mbid = gid)

clean_tracks <- left_join(clean_tracks, ab_stamps, "track_mbid") %>%
  data.table()

clean_tracks <-
  left_join(clean_tracks, select(clean_singles, mbid, year), "mbid") %>%
  data.table()

clean_tracks$submit_delay <-
  clean_tracks$submit_year - clean_tracks$year

# find tracks not on acousticbrainz (to be added manually)

set.seed(123)

check_tracks <-
  tracks[!complete.cases(tracks), c("mbid", "track_mbid")] %>%
  sample_n(200) %>%
  data.table()

track_details <-
  foreach(i = check_tracks$track_mbid, .combine = "rbind") %do% {
    details <- paste0("https://musicbrainz.org/recording/", i) %>%
      read_html() %>%
      html_nodes(".recordingheader") %>%
      html_nodes("bdi") %>%
      html_text()
    data.table("mb_title" = details[1], "mb_artist" = details[2])
  }

check_tracks <- cbind(check_tracks, track_details)

# retrieve features of manually added tracks

check_tracks <- paste0(paths$extra, "checktracks.csv") %>%
  read.csv(as.is = T) %>%
  data.table()

check_tracks <- check_tracks[yt_link != "", c("mbid", "track_mbid")]

check_25 <-
  split(check_tracks$track_mbid, as.integer((seq_along(
    check_tracks$track_mbid
  ) - 1) / 25))

registerDoParallel(clust)
check_feats <-
  foreach(i = check_25, .combine = "rbind") %dopar% {
    ids <- unlist(i) %>%
      as.character() %>%
      paste(collapse = ":0;")
    json <-
      paste("https://acousticbrainz.org/api/v1/low-level?recording_ids",
            ids,
            sep = "=") %>% read_json()
    Sys.sleep(1)
    found <- attributes(json) %>%
      unlist() %>%
      as.character()
    all_feats <- data.table(
      "track_mbid" = as.character(),
      "track_title" = as.character(),
      "length" = as.character(),
      "key_key" = as.character(),
      "key_scale" = as.character(),
      "key_strength" = as.character(),
      "chords_key" = as.character(),
      "chords_scale" = as.character(),
      "danceability" = as.character(),
      "bpm" = as.character(),
      "beats_count" = as.character(),
      "average_loudness" = as.character(),
      "dynamic_complexity" = as.character(),
      "chords_changes_rate" = as.character(),
      "chords_number_rate" = as.character()
    )
    for (j in found) {
      this_json <- json[[j]]$`0`
      feats <-
        c(
          j,
          this_json$metadata$tags$title,
          this_json$metadata$audio_properties$length,
          this_json$tonal$key_key,
          this_json$tonal$key_scale,
          this_json$tonal$key_strength,
          this_json$tonal$chords_key,
          this_json$tonal$chords_scale,
          this_json$rhythm$danceability,
          this_json$rhythm$bpm,
          this_json$rhythm$beats_count,
          this_json$lowlevel$average_loudness,
          this_json$lowlevel$dynamic_complexity,
          this_json$tonal$chords_changes_rate,
          this_json$tonal$chords_number_rate
        )
      feats <- as.matrix(feats) %>%
        t() %>%
        data.table()
      if (ncol(feats) == ncol(all_feats)) {
        colnames(feats) <- colnames(all_feats)
        all_feats <- rbind(all_feats, feats)
      }
    }
    distinct(all_feats)
  }
stopImplicitCluster()

check_feats <- apply(check_feats, 2, unlist) %>%
  data.table()

check_tracks <-
  left_join(check_tracks, check_feats, "track_mbid") %>%
  data.table()

# dummify nominal features and compute submission delays

check_tracks <-
  dummy_cols(
    check_tracks[complete.cases(check_tracks)],
    select_columns = c("key_key", "key_scale", "chords_key", "chords_scale"),
    remove_first_dummy = T,
    remove_selected_columns = T
  )

check_tracks <-
  cbind(check_tracks[, 1:3], apply(check_tracks[, -c(1:3)], 2, as.numeric))

check_tracks$submit_year <- 2020

check_tracks <-
  left_join(check_tracks, singles[, c("mbid", "year")], "mbid") %>%
  data.table()

check_tracks$submit_delay <-
  check_tracks$submit_year - check_tracks$year

# generate comparison data table

compare_years <- unique(na.omit(clean_tracks$submit_year))

set.seed(620)

compare_tracks <-
  clean_tracks[year %in% compare_years & submit_delay >= 0] %>%
  sample_n(620) %>%
  rbind(check_tracks)

compare_tracks$added <-
  ifelse(compare_tracks$submit_year == 2020, 1, 0)

# multidimensional scaling solution of track features

compare_tracks <-
  cbind(compare_tracks, cmdscale(dist(compare_tracks[, ..feats]), 2))

# various functions to simplify later processing

read_mbcsv <- function(x, col.names) {
  read.csv(
    paste0(paths$musicbrainz, x),
    header = F,
    sep = "\t",
    quote = "",
    row.names = NULL,
    col.names = col.names,
    stringsAsFactors = F
  ) %>% data.table()
}

batch_split <- function(x, y) {
  split(x, ceiling(seq_along(x) / y))
}

loop_report <- function(x, batches = NULL) {
  if (!is.null(batches)) {
    p <- round(100 * x / length(batches), 2)
    format(Sys.time(), "%H:%M:%S") %>%
      paste0(" loop ", x, " (", p, "%)") %>%
      message()
  } else {
    format(Sys.time(), "%H:%M:%S") %>%
      paste0(" loop ", x) %>%
      message()
  }
}

extract_master <- function(x) {
  x <- x$relations
  if (length(x) > 0) {
    as.character(x) %>%
      unlist() %>%
      paste(collapse = "") %>%
      str_extract("master/[[:digit:]]*") %>%
      str_remove("master/") %>%
      as.integer()
  } else {
    NA
  }
}

extract_date <- function(x) {
  x <- x$`first-release-date`
  if (length(x) > 0 & nchar(x) > "") {
    x
  } else {
    NA
  }
}

extract_attribute <- function(x, y) {
  p1 <- paste0("<", y, ">.*</", y, ">")
  p2 <- paste0("</?", y, ">")
  x <- master[master_id == x] %>%
    as.character() %>%
    str_extract_all(p1) %>%
    unlist() %>%
    str_remove_all(p2)
  if (length(x) > 0) {
    paste(x, collapse = "~")
  } else {
    NA
  }
}

release_stamp <- function(x) {
  y <- str_extract(x, "^[[:digit:]]{4}")
  m <- str_extract(x, "^[[:digit:]]{4}-[[:digit:]]{2}") %>%
    str_remove("[[:digit:]]{4}-")
  d <-
    str_extract(x, "^[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}") %>%
    str_remove("[[:digit:]]{4}-[[:digit:]]{2}-")
  paste(y, m, d, "00:00:00") %>%
    strptime(format = "%Y %m %d %H:%M:%S", tz = "UTC") %>%
    as.numeric()
}

browse_discogs <- function(x) {
  x <- paste0("https://www.discogs.com/", x)
  rd$navigate(x)
  wait_load("css", "body")
  currenturl <- rd$getCurrentUrl()[[1]]
  if (grepl("/login", currenturl)) {
    user <- rd$findElement("id", "username")
    user$clearElement()
    user$sendKeysToElement(list(login$user))
    pass <- rd$findElement("id", "password")
    pass$clearElement()
    pass$sendKeysToElement(list(login$pass, "\uE007"))
    Sys.sleep(12)
    wait_load("css", "body")
  } else {
    runif(1, 1, 2) %>%
      Sys.sleep()
  }
}

wait_load <- function(x, y) {
  test <- NULL
  while (is.null(load)) {
    test <- tryCatch({
      driver$findElement(x, y)
    },
    error = function(e) {
      NULL
    })
  }
}

normalize_tag <- function(x) {
  stri_unescape_unicode(x) %>%
    iconv("UTF-8", "ASCII//TRANSLIT") %>%
    str_replace_all(", ", ",") %>%
    str_replace_all("[^[:alpha:],]", "_") %>%
    str_replace_all("_{2,}", "_") %>%
    tolower()
}

patternize_tag <- function(x) {
  paste0("^", x, ",|,", x, ",|,", x, "$|^", x, "$")
}

# extract again single data musicbrainz dump

relgroup_csv <-
  read_mbcsv(
    "release_group",
    c(
      "id",
      "gid",
      "name",
      "artist_credit",
      "type",
      "comment",
      "edits_pending",
      "last_updated"
    )
  ) %>%
  select(id, gid, title = name, artist_credit, type)

relgroup_type <-
  read_mbcsv("release_group_primary_type", c("id", "name", rep(NA, 4))) %>%
  select(id, name) %>%
  mutate(id = as.character(id), name = tolower(name))

relgroup_json <- paste0(paths$musicbrainz, "release-group") %>%
  readLines(warn = F)

master <- paste0(paths$discogs, "masters.xml") %>%
  read_xml() %>%
  xml_children()

master_id <- xml_attr(master, "id") %>%
  as.integer()

batch <- batch_split(1:length(relgroup_json), 500)
tmp <- list()

for (i in 1:length(batch)) {
  loop_report(i, batch)
  registerDoParallel(clust)
  tmp[[i]] <-
    foreach(j = batch[[i]], .combine = rbind) %dopar% {
      json <- parse_json(relgroup_json[j])
      master <- extract_master(json)
      if (!is.na(master)) {
        data.table(
          gid = json$id,
          master = master,
          release_date = extract_date(json),
          main_release = extract_attribute(master, "main_release")
        )
      }
    }
  stopImplicitCluster()
}

single <- Reduce(rbind, tmp) %>%
  left_join(relgroup_csv, by = "gid") %>%
  left_join(relgroup_type, by = c("type" = "id")) %>%
  subset(name == "single") %>%
  select(id,
         gid,
         master,
         title,
         artist_credit,
         release_date,
         main_release) %>%
  data.table()

single$release_stamp <-
  mcmapply(release_stamp, single$release_date, mc.cores = clust)

# scrape full discogs revision histories

single <-
  mutate(single, year = as.integer(str_extract(release_date, "^[[:digit:]]{4}"))) %>%
  subset(year >= 2000) %>%
  subset(!is.na(main_release))

if (!dir.exists(paste0(paths$discogs, "release/"))) {
  paste0(paths$discogs, "release/") %>%
    dir.create()
}

registerDoParallel(clust)
missing <- foreach(i = single$main_release, .combine = c) %dopar% {
  dir <- paste0(paths$discogs, "release") %>%
    list.dirs(recursive = F)
  dir <- dir[grepl(paste0("/", i, "-"), dir)]
  if (length(dir) != 1) {
    T
  } else {
    error <- paste0(dir, "/history.html") %>%
      read_html() %>%
      html_nodes(xpath = "//img[contains(@src, 'maintenance')]")
    if (length(error) > 0) {
      T
    } else {
      last_listed <- paste0(dir, "/history.html") %>%
        read_html() %>%
        html_nodes(xpath = "//a[contains(@href, 'history?rev=')]") %>%
        html_attr("href") %>%
        str_extract("rev=[[:digit:]]*") %>%
        str_remove("rev=") %>%
        as.integer() %>%
        max()
      last_scraped <- list.files(dir, recursive = T) %>%
        str_extract("rev=[[:digit:]]*") %>%
        str_remove("rev=") %>%
        as.integer() %>%
        max(na.rm = T)
      if (last_scraped < last_listed) {
        T
      } else {
        F
      }
    }
  }
}
stopImplicitCluster()

toscrape <- single[missing]$main_release

rs <-
  rsDriver(
    browser = "firefox",
    version = list_versions("seleniumserver")$generic[1],
    port = 4445L,
    verbose = F
  )

rd <- rs[["client"]]

batch <- 1:length(toscrape)

for (i in batch) {
  loop_report(i, batch)
  id <- toscrape[i]
  paste0("release/", id, "/history") %>%
    browse_discogs()
  landing <- rd$getCurrentUrl()[[1]]
  title <-
    str_remove_all(landing, "https://www.discogs.com/release/|/history") %>%
    URLdecode()
  subdir <- paste0(path$discogs, "release/", title)
  if (!dir.exists(subdir)) {
    dir.create(subdir)
    paste0(subdir, "/images") %>%
      dir.create()
  }
  landing <- rd$getPageSource()[[1]]
  write(landing, file = paste0(subdir, "/history.html"))
  currentpage <- read_html(landing) %>%
    html_nodes("span.currentpage") %>%
    html_text() %>%
    as.integer()
  if (length(currentpage) < 1) {
    currentpage <- 1L
  }
  for (p in 1:currentpage) {
    paste0("release/", title, "/history?page=", p) %>%
      browse_discogs()
    page <- rd$getPageSource()[[1]]
    write(page, file = paste0(subdir, "/history?page=", p, ".html"))
    href <- read_html(page) %>%
      html_nodes(xpath = "//a[contains(@href, 'history?rev=')]") %>%
      html_attr("href") %>%
      str_remove("^/")
    for (h in href) {
      browse_discogs(h)
      rev <- rd$getPageSource()[[1]]
      if (grepl("/images", h)) {
        write(rev, file = paste0(subdir, "/images", str_extract(h, "/[^//]*$"), ".html"))
      } else {
        write(rev, file = paste0(subdir, str_extract(h, "/[^//]*$"), ".html"))
      }
    }
  }
}

rs$server$stop()

scraped <- paste0(path$discogs, "release") %>%
  list.dirs(recursive = F)

scraped_id <-  str_extract(scraped, "release/[[:digit:]]*-") %>%
  str_remove_all("release/|-")

registerDoParallel(clust)
history <- foreach(i = 1:nrow(single), .combine = rbind) %dopar% {
  id <- single[i]$id
  main_release <- single[i]$main_release
  if (main_release %in% scraped_id) {
    subdir <- scraped[which(scraped_id == main_release)]
    revs <- list.files(subdir, recursive = T) %>%
      str_subset("rev=[[:digit:]]*&page")
    if (length(revs) < 1) {
      data.table(
        id,
        main_release,
        rev = NA,
        user = NA,
        decision_stamp = NA,
        genre = NA,
        style = NA
      )
    } else {
      rev_sort <- str_extract(revs, "rev=[[:digit:]]*") %>%
        str_remove("rev=") %>%
        as.integer()
      revs <- data.table(revs, rev_sort)[order(rev_sort)]$revs
      revs <- paste0(subdir, "/", revs)
      foreach(k = revs, .combine = rbind) %do% {
        html <- read_html(k)
        rev <- str_extract(k, "rev=[[:digit:]]*") %>%
          str_remove("rev=") %>%
          as.integer()
        user <- html_nodes(html, "tr.datarow.highlight") %>%
          html_nodes("a.linked_username") %>%
          html_text() %>%
          str_remove_all("\n") %>%
          trimws()
        if (length(user) < 1) {
          user <- NA
        }
        decision_stamp <-
          html_nodes(html, "tr.datarow.highlight") %>%
          html_nodes("span") %>%
          html_attrs() %>%
          unlist() %>%
          str_subset("AM|PM")
        profile <- html_nodes(html, "div.profile.noimg") %>%
          html_children()
        genre <-
          profile[str_which(as.character(profile), "Genre:") + 1] %>%
          html_text() %>%
          str_remove_all("\n") %>%
          trimws() %>%
          str_replace("Folk, World, & Country", "Folk World & Country") %>%
          normalize_tag()
        genre <- ifelse(nchar(genre) > 1, genre, NA)
        style <-
          profile[str_which(as.character(profile), "Style:") + 1] %>%
          html_text() %>%
          str_remove_all("\n") %>%
          trimws() %>%
          str_replace("R&B/Swing", "RnB/Swing") %>%
          str_replace("Shoegazer", "Shoegaze") %>%
          normalize_tag()
        style <- ifelse(nchar(style) > 1, style, NA)
        data.table(id,
                   main_release,
                   rev,
                   user,
                   decision_stamp,
                   genre,
                   style)
      }
    }
  }
}
stopImplicitCluster()

history <- distinct(history) %>%
  mutate(decision_date = as.POSIXct(strptime(
    decision_stamp, format = "%d-%b-%y %I:%M %p", tz = "UTC"
  ))) %>%
  mutate(decision_stamp = as.numeric(decision_date))

# calculate users' expertise in genres and styles

singles$stamp_unix <-
  with(singles, ISOdate(stamp_year, stamp_month, stamp_day)) %>%
  as.numeric()

single_styles <-
  left_join(single_styles,
            singles[, c("mbid", "user", "stamp_unix")],
            by = c("single" = "mbid", "user"))

single_genres <-
  left_join(single_genres,
            singles[, c("mbid", "user", "stamp_unix")],
            by = c("single" = "mbid", "user"))

user_expertise <- function(u, t, g, s) {
  g <- normalize_tag(g)
  s <- normalize_tag(s)
  any <- history[user == u & decision_stamp < t]
  same_genre <- any[str_detect(genre, patternize_tag(g))]
  same_style <- same_genre[str_detect(style, patternize_tag(s))]
  data.table(
    expertise_any = nrow(any),
    expertise_genre = nrow(same_genre),
    expertise_style = nrow(same_style)
  )
}

batch <- batch_split(1:nrow(single_styles), 36000)
tmp <- list()

for (i in 1:length(batch)) {
  loop_report(i, batch)
  registerDoParallel(clust)
  tmp[[i]] <- foreach(j = batch[[i]], .combine = rbind) %dopar% {
    with(single_styles[j],
         user_expertise(user, stamp_unix, genre, style))
  }
  stopImplicitCluster()
}

single_styles <- cbind(single_styles, Reduce(rbind, tmp))

batch <- batch_split(1:nrow(single_genres), 36000)
tmp <- list()

for (i in 1:length(batch)) {
  loop_report(i, batch)
  registerDoParallel(clust)
  tmp[[i]] <- foreach(j = batch[[i]], .combine = rbind) %dopar% {
    with(single_genres[j],
         user_expertise(user, stamp_unix, genre, NA))
  }
  stopImplicitCluster()
}

single_genres <- cbind(single_genres, Reduce(rbind, tmp)[, 1:2])

# join release timestamps

single_styles <-
  left_join(single_styles,
            single[, c("gid", "release_stamp")],
            by = c("single" = "gid"))

single_genres <-
  left_join(single_genres,
            single[, c("gid", "release_stamp")],
            by = c("single" = "gid"))

# identify genre and style exemplars

read_listens <- function(x) {
  files <- paste0(paths$listenbrainz, x) %>%
    list.files(full.names = T)
  ceiling(clust * .75) %>%
    registerDoParallel()
  dump <-
    foreach(i = files, .combine = rbind) %dopar% {
      raw <- readLines(i)
      stamps <- str_extract(raw, '\"timestamp\":[^,]+') %>%
        str_extract("[[:digit:]]+") %>%
        as.integer()
      data.table(
        user = str_extract(raw, '\"user_name\":[^,]+') %>%
          str_remove_all("\"user_name\":|\""),
        mbid = str_extract(raw, '\"recording_mbid\":[^,]+') %>%
          str_extract("[[:alnum:]-]{36}"),
        lstamp = stamps,
        lyear = x,
        lmonth = as.Date.POSIXct(stamps) %>%
          month(),
        lday = as.Date.POSIXct(stamps) %>%
          mday()
      )
    }
  stopImplicitCluster()
  if (!is.null(dump)) {
    dump <- dump[mbid %in% clean_tracks$track_mbid]
    if (nrow(dump) > 0) {
      dump[order(lstamp)]
    }
  }
}

obstime <- singles$stamp_year %>%
  unique() %>%
  sort()

listens <-
  data.table(
    user = as.character(),
    mbid = as.character(),
    lstamp = as.numeric(),
    lyear = as.integer(),
    lmonth = as.integer(),
    lday = as.integer()
  )

for (i in obstime) {
  loop_report(i)
  listens <- rbind(listens, read_listens(i))
}

listens <-
  rename(clean_tracks[, c("mbid", "track_mbid")], single_mbid = mbid) %>%
  right_join(listens, by = c("track_mbid" = "mbid"))

style_exemplars <- function(t, g, s) {
  any <- history[decision_stamp < t]
  same_genre <- any[str_detect(genre, patternize_tag(g))]
  same_style <- same_genre[str_detect(style, patternize_tag(s))]
  latest <- group_by(same_style, id) %>%
    slice_max(decision_stamp) %>%
    data.table()
  latest_mbids <- singles[mainrel %in% latest$main_release]$mbid
  all_listens <- listens[lstamp < t]
  latest_listens <- foreach(i = latest_mbids, .combine = c) %do% {
    nrow(all_listens[single_mbid == i])
  }
  latest_features <-
    foreach(i = latest_mbids, .combine = rbind) %do% {
      data.table(mbid = i) %>%
        cbind(clean_singles[mbid == i, ..feats])
    }
  list(features = latest_features,
       listens = latest_listens)
}

genre_exemplars <- function(t, g) {
  any <- history[decision_stamp < t]
  same_genre <- any[str_detect(genre, patternize_tag(g))]
  latest <- group_by(same_genre, id) %>%
    slice_max(decision_stamp) %>%
    data.table()
  latest_mbids <- singles[mainrel %in% latest$main_release]$mbid
  all_listens <- listens[lstamp < t]
  latest_listens <- foreach(i = latest_mbids, .combine = c) %do% {
    nrow(all_listens[single_mbid == i])
  }
  latest_features <-
    foreach(i = latest_mbids, .combine = rbind) %do% {
      data.table(mbid = i) %>%
        cbind(clean_singles[mbid == i, ..feats])
    }
  list(features = latest_features,
       listens = latest_listens)
}

# calculate singles' distance to genre and style exemplars

style_covmat <- function(s, g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  style_mainrels <- mainrels[grepl(paste(
    paste("^", s, ",", sep = ""),
    paste(", ", s, ",", sep = ""),
    paste(", ", s, "$", sep = ""),
    paste("^", s, "$", sep = ""),
    sep = "|"
  ), style) & stamp_year == y & grepl(g, genre)]$mainrel
  style_feats <-
    clean_singles[mainrel %in% style_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(style_feats) < 2) {
    NA
  } else {
    cov(style_feats) + diag(ncol(style_feats)) * .001
  }
}

genre_covmat <- function(g, y, lag = T) {
  if (lag == T) {
    y <- y - 1
  }
  genre_mainrels <-
    mainrels[stamp_year == y & grepl(g, genre)]$mainrel
  genre_feats <-
    clean_singles[mainrel %in% genre_mainrels, ..feats] %>%
    as.matrix()
  if (nrow(genre_feats) < 2) {
    NA
  } else {
    cov(genre_feats) + diag(ncol(genre_feats)) * .001
  }
}

exemplar_distance <- function(x, t, g, s) {
  if (year(anytime(t)) < 2006) {
    exit <- "no listens"
    exemplar_dist <- NA
  } else {
    if (is.na(s)) {
      covmat <- genre_covmat(g, year(anytime(t)))
    } else {
      covmat <- style_covmat(s, g, year(anytime(t)))
    }
    if (is.na(sum(covmat))) {
      exit <- "not enough instances"
      exemplar_dist <- NA
    } else {
      g <- normalize_tag(g)
      if (!is.na(s)) {
        s <- normalize_tag(s)
      }
      single_feats <- clean_singles[mbid == x, ..feats] %>%
        as.matrix()
      if (is.na(s)) {
        exemplar <- genre_exemplars(t, g)
      } else {
        exemplar <- style_exemplars(t, g, s)
      }
      if (!is.null(exemplar$features)) {
        keep <- complete.cases(exemplar$features)
        if (sum(keep) > 0) {
          features <- exemplar$features[keep, ..feats] %>%
            as.matrix()
          if (sum(exemplar$listens[keep]) > 0) {
            weights <-
              exemplar$listens[keep] / sum(exemplar$listens[keep])
          } else {
            weights <-
              rep(1 / length(exemplar$listens[keep]),
                  length(exemplar$listens[keep]))
          }
          distances <-
            foreach(i = 1:nrow(features), .combine = c) %do% {
              exemplar_feats <- as.matrix(features[i,]) %>% t()
              mahalanobis(single_feats, exemplar_feats, covmat) %>%
                sqrt()
            }
          exit <- "ok"
          exemplar_dist <- sum(distances * weights)
        } else {
          exit <- "no exemplars with feats"
          exemplar_dist <- NA
        }
      } else {
        exit <- "no exemplars"
        exemplar_dist <- NA
      }
    }
  }
  data.table(exit, exemplar_dist)
}

batch <- batch_split(1:nrow(single_styles), 500)
tmp <- list()

for (i in 1:length(batch)) {
  loop_report(i, batch)
  registerDoParallel(clust)
  tmp[[i]] <- foreach(j = batch[[i]], .combine = rbind) %dopar% {
    with(single_styles[j],
         exemplar_distance(single, stamp_unix, genre, style))
  }
  stopImplicitCluster()
}

single_styles <-
  cbind(single_styles, exemplar_dist = Reduce(rbind, tmp)$exemplar_dist)

batch <- batch_split(1:nrow(single_genres), 500)
tmp <- list()

for (i in 1:length(batch)) {
  loop_report(i, batch)
  registerDoParallel(clust)
  tmp[[i]] <- foreach(j = batch[[i]], .combine = rbind) %dopar% {
    with(single_genres[j],
         exemplar_distance(single, stamp_unix, genre, NA))
  }
  stopImplicitCluster()
}

single_genres <-
  cbind(single_genres, exemplar_dist = Reduce(rbind, tmp)$exemplar_dist)

# count previous assignments of genres and styles

previous_tags <- function(t, g, s) {
  g <- normalize_tag(g)
  any <- history[decision_stamp < t]
  same_genre <- any[str_detect(genre, patternize_tag(g))]
  if (!is.na(s)) {
    s <- normalize_tag(s)
    same_style <- same_genre[str_detect(style, patternize_tag(s))]
  } else {
    same_style <- same_genre
  }
  nrow(same_style)
}

batch <- batch_split(1:nrow(single_styles), 36000)
tmp <- list()

for (i in 1:length(batch)) {
  loop_report(i, batch)
  registerDoParallel(clust)
  tmp[[i]] <- foreach(j = batch[[i]], .combine = rbind) %dopar% {
    with(single_styles[j],
         previous_tags(stamp_unix, genre, style))
  }
  stopImplicitCluster()
}

single_styles <-
  cbind(single_styles, previous_tags = Reduce(c, tmp))

batch <- batch_split(1:nrow(single_genres), 36000)
tmp <- list()

for (i in 1:length(batch)) {
  loop_report(i, batch)
  registerDoParallel(clust)
  tmp[[i]] <- foreach(j = batch[[i]], .combine = rbind) %dopar% {
    with(single_genres[j],
         previous_tags(stamp_unix, genre, NA))
  }
  stopImplicitCluster()
}

single_genres <-
  cbind(single_genres, previous_tags = Reduce(c, tmp))

# save prepared data (checkpoint)

save(
  list = c(
    "clean_singles",
    "clean_tracks",
    "compare_tracks",
    "feats",
    "single_genres",
    "single_styles",
    "singles"
  ),
  file = "path/to/checkpoint.RData"
)
