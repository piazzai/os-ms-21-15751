# load libraries

library(car)
library(dplyr)
library(data.table)
library(doParallel)
library(Hmisc)
library(survival)
library(ggplot2)
library(lmtest)
library(LaplacesDemon)

# set number of cores for parallel processing (default: half of system cores)

clust <- ceiling(detectCores() / 2)

# load data after extracting the tarball

load("checkpoint.RData")

# function to report pairwise correlations

create_cormat <- function(x) {
  x <- as.matrix(x)
  R <- rcorr(x)$r
  p <- rcorr(x)$P
  mystars <-
    ifelse(p < .001, "***",
           ifelse(p < .01, "**",
                  ifelse(p < .05, "*",
                         ifelse(p < .1, "+", ""))))
  R <- format(round(cbind(rep(-1.11, ncol(
    x
  )), R), 2))[,-1]
  Rnew <- matrix(paste(R, mystars, sep = ""), ncol = ncol(x))
  diag(Rnew) <- paste(diag(R), " ", sep = "")
  rownames(Rnew) <- colnames(x)
  colnames(Rnew) <- paste(colnames(x), "", sep = "")
  Rnew <- as.matrix(Rnew)
  Rnew[upper.tri(Rnew, diag = TRUE)] <- ""
  Rnew <- as.data.frame(Rnew)
  Rnew <- cbind(Rnew[1:length(Rnew) - 1])
  P <- format(round(cbind(rep(-1.11, ncol(
    x
  )), p), 3))[,-1]
  Pnew <- matrix(P, ncol = ncol(x))
  diag(Pnew) <- paste(diag(P), " ", sep = "")
  rownames(Pnew) <- colnames(x)
  colnames(Pnew) <- paste(colnames(x), "", sep = "")
  Pnew <- as.matrix(Pnew)
  Pnew[upper.tri(Pnew, diag = TRUE)] <- ""
  Pnew <- as.data.frame(Pnew)
  Pnew <- cbind(Pnew[1:length(Pnew) - 1])
  return(list(Rnew, Pnew))
}

# function to compute condition indices

colldiag <-
  function(mod,
           scale = TRUE,
           center = FALSE,
           add.intercept = TRUE) {
    result <- NULL
    if (center)
      add.intercept <- FALSE
    if (is.matrix(mod) || is.data.frame(mod)) {
      X <- as.matrix(mod)
      nms <- colnames(mod)
    }
    else if (!is.null(mod$call$formula)) {
      X <- mod$model[,-1]
    }
    X <- na.omit(X)
    if (add.intercept) {
      X <- cbind(1, X)
      colnames(X)[1] <- "intercept"
    }
    X <- scale(X, scale = scale, center = center)
    
    svdX <- svd(X)
    svdX$d
    condindx <- svdX$d[1] / svdX$d
    
    Phi = svdX$v %*% diag(1 / svdX$d)
    Phi <- t(Phi ^ 2)
    pi <- prop.table(Phi, 2)
    
    dim(condindx) <- c(length(condindx), 1)
    colnames(condindx) <- "cond.index"
    rownames(condindx) <- 1:nrow(condindx)
    colnames(pi) <- colnames(X)
    result$condindx <- condindx
    result$pi <- pi
    class(result) <- "colldiag"
    result
  }

# compute descriptives of single-level acoustic features

sample_mbids <-
  singles[mbid %in% clean_singles$mbid &
            year == stamp_year & complete.cases(singles)]$mbid

descr_singles <-
  data.frame(
    "mean" = apply(clean_singles[mbid %in% sample_mbids, ..feats], 2, mean),
    "sdev" = apply(clean_singles[mbid %in% sample_mbids, ..feats], 2, sd),
    "min" = apply(clean_singles[mbid %in% sample_mbids, ..feats], 2, min),
    "max" = apply(clean_singles[mbid %in% sample_mbids, ..feats], 2, max)
  ) %>% round(3)

rownames(descr_singles) <- feats

cormat_singles <-
  create_cormat(clean_singles[mbid %in% sample_mbids, ..feats])

# compute descriptives of track-level acoustic features

descr_tracks <-
  data.frame(
    "mean" = apply(clean_tracks[mbid %in% sample_mbids, ..feats], 2, mean),
    "sdev" = apply(clean_tracks[mbid %in% sample_mbids, ..feats], 2, sd),
    "min" = apply(clean_tracks[mbid %in% sample_mbids, ..feats], 2, min),
    "max" = apply(clean_tracks[mbid %in% sample_mbids, ..feats], 2, max)
  ) %>% round(3)

rownames(descr_tracks) <- feats

cormat_tracks <-
  create_cormat(clean_tracks[mbid %in% sample_mbids, ..feats])

# calculate categorization delays, choice sets, and clusters

single_styles$tag_delay <-
  single_styles$tag_year - single_styles$rel_year

single_styles$choice_set <- factor(single_styles$single) %>%
  as.integer()

single_styles$user_label <-
  paste(single_styles$user, single_styles$style, sep = ", ") %>%
  factor() %>%
  as.integer()

single_genres$tag_delay <-
  single_genres$tag_year - single_genres$rel_year

single_genres$choice_set <- factor(single_genres$single) %>%
  as.integer()

single_genres$user_label <-
  paste(single_genres$user, single_genres$genre, sep = ", ") %>%
  factor() %>%
  as.integer()

# subsamples where categorization decisions require shorter delays

single_styles$delay <-
  with(single_styles, stamp_unix - release_stamp) / 86400

single_styles$subsample_6m <-
  with(single_styles, delay >= 0 & delay < 180)

single_styles$subsample_1m <-
  with(single_styles, delay >= 0 & delay < 30)

single_styles$subsample_1w <-
  with(single_styles, delay >= 0 & delay < 7)

# subsamples where choice sets only include nearest styles

atypicality_quantiles <- function(x) {
  single_styles[single == x]$atypicality %>%
    quantile(seq(0, 1, .01))
}

registerDoParallel(clust)
tmp <-
  foreach(i = 1:nrow(single_styles), .combine = rbind) %dopar% {
    q <- atypicality_quantiles(single_styles[i]$single)
    a <- single_styles[i]$atypicality
    data.table(
      subsample_q75 = (a <= q[75 + 1]),
      subsample_q50 = (a <= q[50 + 1]),
      subsample_q25 = (a <= q[25 + 1])
    )
  }
stopImplicitCluster()

single_styles <- cbind(single_styles, tmp)

# subsample without top users

top_users <- table(singles[mbid %in% sample_mbids]$user) %>%
  sort() %>%
  tail(3) %>%
  names()

top_mbids <-
  singles[mbid %in% sample_mbids & user %in% top_users]$mbid

single_styles$subsample_top <-
  !(single_styles$single %in% top_mbids)

# recode and standardize variables

dvs <- c("tagged", "choice_set", "user_label")

ivs <-
  c(
    "typicality",
    "selfclaim_osa",
    "expertise_style",
    "expertise_genre",
    "informative",
    "distinctive"
  )

alt_ivs <-
  c("previous_tags",
    "exemplarity",
    "selfclaim_lv",
    "selfclaim_cos",
    "selfclaim_jw")

svs <- c(
  "subsample_6m",
  "subsample_1m",
  "subsample_1w",
  "subsample_q75",
  "subsample_q50",
  "subsample_q25",
  "subsample_top"
)

recode_variables_clogit <- function(x, std = T) {
  all_ivs <- c(ivs, alt_ivs)
  x$typicality <- ifelse(x$atypicality > 0, 1 / x$atypicality, NA)
  x$exemplarity <-
    ifelse(x$exemplar_dist > 0, 1 / x$exemplar_dist, NA)
  x$selfclaim_lv <-
    min(x$selfclaim_lv) + max(x$selfclaim_lv) - x$selfclaim_lv
  x$selfclaim_osa <-
    min(x$selfclaim_osa) + max(x$selfclaim_osa) - x$selfclaim_osa
  x$selfclaim_cos <-
    min(x$selfclaim_cos, na.rm = T) + max(x$selfclaim_cos, na.rm = T) - x$selfclaim_cos
  x$selfclaim_jw <-
    min(x$selfclaim_jw) + max(x$selfclaim_jw) - x$selfclaim_jw
  if (sum(grepl("subsample", colnames(x))) > 0) {
    cbind(x[, ..dvs], scale(x[, ..all_ivs], scale = std, center = std), x[, ..svs])
  } else {
    cbind(x[, ..dvs], scale(x[, ..all_ivs], scale = std, center = std))
  }
}

dataset_styles_unsd <-
  recode_variables_clogit(single_styles[tag_delay == 0], F)

dataset_styles <-
  recode_variables_clogit(single_styles[tag_delay == 0])

single_genres$expertise_style <- NA

dataset_genres_unsd <-
  recode_variables_clogit(single_genres[tag_delay == 0], F)

dataset_genres <-
  recode_variables_clogit(single_genres[tag_delay == 0])

# compute descriptives of regression variables

descr_vs <- c("tagged", ivs)

descr_reg_styles <- data.frame(
  mean = apply(dataset_styles_unsd[, ..descr_vs], 2, mean, na.rm = T),
  sd = apply(dataset_styles_unsd[, ..descr_vs], 2, sd, na.rm = T),
  min = apply(dataset_styles_unsd[, ..descr_vs], 2, min, na.rm = T),
  max = apply(dataset_styles_unsd[, ..descr_vs], 2, max, na.rm = T)
) %>% round(3)

cormat_reg_styles <-
  create_cormat(dataset_styles_unsd[, ..descr_vs])

descr_vs <- setdiff(descr_vs, "expertise_style")

descr_reg_genres <- data.frame(
  mean = apply(dataset_genres_unsd[, ..descr_vs], 2, mean, na.rm = T),
  sd = apply(dataset_genres_unsd[, ..descr_vs], 2, sd, na.rm = T),
  min = apply(dataset_genres_unsd[, ..descr_vs], 2, min, na.rm = T),
  max = apply(dataset_genres_unsd[, ..descr_vs], 2, max, na.rm = T)
) %>% round(3)

cormat_reg_genres <-
  create_cormat(dataset_genres_unsd[, ..descr_vs])

# function to report conditional logit estimates

report_clogit <- function(model, data, previous = NULL) {
  est <- summary(model)$coefficients[, c(1, 4, 6)] %>%
    round(3)
  or <-
    with(summary(model), cbind(
      or = exp(coefficients[, 1]),
      cilow = exp(coefficients[, 1] - 1.96 * coefficients[, 4]),
      cihigh = exp(coefficients[, 1] + 1.96 * coefficients[, 4])
    )) %>%
    round(3)
  if (!is.null(previous)) {
    lr <- lrtest(model, previous)
  } else {
    lr <- NULL
  }
  n_obs <- nrow(data)
  n_clust <- unique(data$user_label) %>% length()
  ll <- logLik(model)
  list(
    estimates = est,
    odds = or,
    no_user_product_pairs = n_obs,
    no_user_label_clusters = n_clust,
    log_likelihood = ll,
    likelihood_ratio = lr
  )
}

# functions to compute odds ratios and log odds

or <- function (model, variable) {
  if (variable %in% names(model$coefficients)) {
    est <- summary(model)$coefficients[variable, c(1, 4, 6)]
    est <- as.numeric(est)
    c(
      or = exp(est[1]),
      cilow = exp(est[1] - 1.96 * est[2]),
      cihigh = exp(est[1] + 1.96 * est[2]),
      p = est[3]
    ) %>% round(3)
  } else {
    NA
  }
}

ordelta <- function(model, variable) {
  d <- deltaMethod(model, variable) %>%
    as.numeric()
  z <- d[1] / d[2]
  p <- exp(-.717 * abs(z) - .416 * abs(z) ^ 2)
  c(
    or = exp(d[1]),
    ci.low = exp(d[3]),
    ci.high = exp(d[4]),
    z = z,
    p = p
  ) %>% round(4)
}

lodelta <- function(model, variable) {
  d <- deltaMethod(model, variable) %>%
    as.numeric()
  z <- d[1] / d[2]
  p <- exp(-.717 * abs(z) - .416 * abs(z) ^ 2)
  c(
    lo = d[1],
    ci.low = d[3],
    ci.high = d[4],
    z = z,
    p = p
  ) %>% round(4)
}

# function to test u-shaped effects

utest <-
  function(model,
           data,
           variable,
           x_min = NULL,
           x_max = NULL) {
    df_total <- length(model$linear.predictors)
    df_model <- length(model$coefficients)
    df_resid <- df_total - df_model
    if (is.null(x_min)) {
      x_min <- min(data[, ..variable]) %>% round(3)
    }
    if (is.null(x_max)) {
      x_max <- max(data[, ..variable]) %>% round(3)
    }
    v <- which(grepl(variable, names(model$coefficients)))
    b1 <- as.numeric(model$coefficients)[v[1]]
    b2 <- as.numeric(model$coefficients)[v[2]]
    s11 <- model$var[v[1], v[1]]
    s12 <- model$var[v[1], v[2]]
    s22 <- model$var[v[2], v[2]]
    sl_min <- b1 + 2 * b2 * x_min
    t_min <-
      sl_min / sqrt(s11 + 4 * x_min ^ 2 * s22 + 4 * x_min * s12)
    p_min <-
      ifelse(t_min > 0,
             pt(t_min, df_resid, lower.tail = F),
             pt(t_min, df_resid)) * 2
    sl_max <- b1 + 2 * b2 * x_max
    t_max <-
      sl_max / sqrt(s11 + 4 * x_max ^ 2 * s22 + 4 * x_max * s12)
    p_max <-
      ifelse(t_max > 0,
             pt(t_max, df_resid, lower.tail = F),
             pt(t_max, df_resid)) * 2
    if (t_min * t_max > 0) {
      cat("Trivial rejection: turning point outside range")
    } else {
      tp <- -b1 / (2 * b2)
      t_alpha <- 1.96
      d <-
        (s12 ^ 2 - s11 * s22) * 1.96 ^ 2 + b2 ^ 2 * s11 + b1 ^ 2 * s22 - 2 * b1 * b2 * s12
      theta_l <-
        (-s12 * 1.96 ^ 2 + b1 * b2 - 1.96 * sqrt(d)) / (b2 ^ 2 - s22 * 1.96 ^ 2)
      theta_h <-
        (-s12 * 1.96 ^ 2 + b1 * b2 + 1.96 * sqrt(d)) / (b2 ^ 2 - s22 * 1.96 ^ 2)
      if (d < 0) {
        fieller <- "[-Inf, +Inf]"
      } else {
        if ((b2 ^ 2 - s22 * 1.96 ^ 2) > 0) {
          fieller <-
            paste0("[",
                   round(-.5 * theta_h, 3),
                   ", ",
                   round(-.5 * theta_l, 3),
                   "]")
        } else {
          fieller <-
            paste0("[-Inf, ",
                   round(-.5 * theta_h),
                   "] U [",
                   round(-.5 * theta_l),
                   ", +Inf]")
        }
      }
      out <- paste0(
        "Slope at x = ",
        round(x_min, 3),
        "\n  ",
        round(sl_min, 3),
        " (t = ",
        round(t_min, 3),
        ", p = ",
        round(p_min, 3),
        ")\nSlope at x = ",
        round(x_max, 3),
        "\n  ",
        round(sl_max, 3),
        " (t = ",
        round(t_max, 3),
        ", p = ",
        round(p_max, 3),
        ")\nTurning point at x = ",
        round(tp, 3),
        "\nFieller interval: ",
        fieller
      )
      list(
        out = out,
        x_min = round(as.numeric(x_min), 3),
        sl_min = round(as.numeric(sl_min), 3),
        t_min = round(as.numeric(t_min), 3),
        p_min = round(as.numeric(p_min), 3),
        x_max = round(as.numeric(x_max), 3),
        sl_max = round(as.numeric(sl_max), 3),
        t_max = round(as.numeric(t_max), 3),
        p_max = round(as.numeric(p_max), 3),
        tp = round(as.numeric(tp), 3),
        fieller = fieller
      )
    }
  }

# estimate conditional logit models

fit1 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre,
    dataset_styles,
    method = "efron"
  )

fit2 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      informative,
    dataset_styles,
    method = "efron"
  )

fit3 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      informative + distinctive,
    dataset_styles,
    method = "efron"
  )

fit4 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles,
    method = "efron"
  )

fit5 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + poly(distinctive, 2),
    dataset_styles,
    method = "efron"
  )

# estimate model of genre assignments

fit6 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_genres,
    method = "efron"
  )

# estimate models where claims use different algorithms

fit7 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_lv + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles,
    method = "efron"
  )

fit8 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_cos + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles,
    method = "efron"
  )

fit9 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_jw + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles,
    method = "efron"
  )

# estimate model that exclude top users

fit10 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles[subsample_top == T],
    method = "efron"
  )

# estimate model that includes the count of previous assignments

fit11 <- clogit(
  tagged ~ strata(choice_set) + cluster(user_label) +
    typicality + selfclaim_osa + expertise_style + expertise_genre +
    previous_tags + poly(informative, 2) + distinctive,
  dataset_styles,
  method = "efron"
)

# estimate models where categorization decisions require shorter delays

fit12 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles[subsample_6m == T],
    method = "efron"
  )

fit13 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles[subsample_1m == T],
    method = "efron"
  )

fit14 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles[subsample_1w == T],
    method = "efron"
  )

# estimate models including distance to exemplars

fit15 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      exemplarity + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles,
    method = "efron"
  )

fit16 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + exemplarity + selfclaim_osa + expertise_style +
      expertise_genre + poly(informative, 2) + distinctive,
    dataset_styles,
    method = "efron"
  )

# estimate models where choice sets are restricted

fit17 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles[subsample_q75 == T],
    method = "efron"
  )

fit18 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles[subsample_q50 == T],
    method = "efron"
  )

fit19 <-
  clogit(
    tagged ~ strata(choice_set) + cluster(user_label) +
      typicality + selfclaim_osa + expertise_style + expertise_genre +
      poly(informative, 2) + distinctive,
    dataset_styles[subsample_q25 == T],
    method = "efron"
  )

# calculate the kld-mahalanobis correlation

random_normal <- function(n, m = 1L, s = 1L) {
  mu <- runif(1, 0, m)
  sigma <- 0
  while (sigma == 0) {
    sigma <- runif(1, 0, s)
  }
  rnd <- rnorm(n, mu, sigma)
  list(mean = mu,
       sd = sigma,
       sample = rnd)
}

kldiv <- function(x, y) {
  space <- c(x$sample, y$sample) %>%
    unique() %>%
    sort()
  px <- dnorm(space, x$mean, x$sd)
  py <- dnorm(space, y$mean, y$sd)
  KLD(px, py)$sum.KLD.py.px
}

mahala <- function(x, y) {
  foreach(i = y$sample, .combine = c) %do% {
    mahalanobis(i, mean(x$sample), var(x$sample)) %>%
      sqrt()
  } %>% mean()
}

registerDoParallel(clust)
kldsim <- foreach(i = 1:1000000, .combine = rbind) %dopar% {
  set.seed(i)
  x <- random_normal(100)
  y <- random_normal(100)
  data.table(
    mean_x = x$mean,
    sd_x = x$sd,
    mean_y = y$mean,
    sd_y = y$sd,
    kldiv = kldiv(x, y),
    mahala = mahala(x, y)
  )
}
stopImplicitCluster()

############################
### REPORTED INFORMATION ###
############################

# results section

descr_vs <- c("tagged", ivs)

colldiag(dataset_styles_unsd[, ..descr_vs])$condindx %>%
  max() %>%
  round(3)

colldiag(dataset_styles[, ..descr_vs])$condindx %>%
  max() %>%
  round(3)

c(
  as.numeric(unlist(lrtest(fit2, fit1)[5])),
  as.numeric(unlist(lrtest(fit3, fit2)[5])),
  as.numeric(unlist(lrtest(fit4, fit3)[5])),
  as.numeric(unlist(lrtest(fit5, fit4)[5]))
) %>% na.omit() %>%
  max() %>%
  round(3)

ordelta(fit4, "typicality") %>%
  round(3)

ordelta(fit4, "selfclaim_osa") %>%
  round(3)

ordelta(fit4, "expertise_style") %>%
  round(3)

ordelta(fit4, "expertise_genre") %>%
  round(3)

ordelta(fit4, "distinctive") %>%
  round(3)

quantile(dataset_styles$informative, c(.01, .99)) %>%
  round(3)

summary(fit4)$coefficients["poly(informative, 2)2", c(2, 4:6)] %>%
  round(3)

utest(
  fit4,
  dataset_styles,
  "informative",
  x_min = quantile(dataset_styles$informative, .01),
  x_max = quantile(dataset_styles$informative, .99)
)$out %>% cat()

summary(fit5)$coefficients["poly(informative, 2)2", c(2, 4:6)] %>%
  round(3)

utest(
  fit5,
  dataset_styles,
  "informative",
  x_min = quantile(dataset_styles$informative, .01),
  x_max = quantile(dataset_styles$informative, .99)
)$out %>% cat()

quantile(dataset_styles$distinctive, c(.01, .99)) %>%
  round(3)

summary(fit5)$coefficients["poly(distinctive, 2)2", c(2, 4:6)] %>%
  round(3)

utest(
  fit5,
  dataset_styles,
  "distinctive",
  x_min = quantile(dataset_styles$distinctive, .01),
  x_max = quantile(dataset_styles$distinctive, .99)
)$out %>% cat()

range(dataset_styles$distinctive) %>%
  round(3)

utest(
  fit5,
  dataset_styles,
  "distinctive",
  x_min = min(dataset_styles$distinctive),
  x_max = max(dataset_styles$distinctive)
)$out %>% cat()

# table 1

cbind(descr_reg_styles, cormat_reg_styles[[1]])

# table 2

report_clogit(fit1, dataset_styles)
report_clogit(fit2, dataset_styles, fit1)
report_clogit(fit3, dataset_styles, fit2)
report_clogit(fit4, dataset_styles, fit3)
report_clogit(fit5, dataset_styles, fit4)

# figure 1

ggplot() +
  geom_point(aes(V1, V2, col = submit_delay), compare_tracks[added == 0], pch = 21) +
  geom_point(aes(V1, V2, pch = factor(added)), compare_tracks[added == 1]) +
  scale_x_continuous(limits = c(-500, 1500),
                     breaks = seq(-500, 1500, length.out = 5)) +
  scale_y_continuous(limits = c(-150, 150),
                     breaks = seq(-150, 150, length.out = 5)) +
  scale_color_continuous(low = "gray80",
                         high = "black",
                         limits = c(0, 5)) +
  scale_shape_manual(
    limits = factor(c(0, 1)),
    values = c(1, 3),
    labels = c("Yes", "No")
  ) +
  labs(x = "Dimension 1",
       y = "Dimension 2",
       shape = "Submitted to\nAcousticBrainz",
       col = "Years before\nsubmission") +
  guides(shape = guide_legend(order = 1), col = guide_colorbar(order = 2)) +
  theme_bw()

# tables 3 and 4

cbind(descr_tracks, cormat_tracks[[1]])

# tables 4 and 5

cbind(descr_singles, cormat_singles[[1]])

# appendix c

with(kldsim, cor.test(kldiv, mahala))

# table 7

report_clogit(fit12, dataset_styles[subsample_6m == T])

utest(
  fit12,
  dataset_styles[subsample_6m == T],
  "informative",
  x_min = quantile(dataset_styles[subsample_6m == T]$informative, .01),
  x_max = quantile(dataset_styles[subsample_6m == T]$informative, .99)
)$out %>% cat()

report_clogit(fit13, dataset_styles[subsample_1m == T])

utest(
  fit13,
  dataset_styles[subsample_1m == T],
  "informative",
  x_min = quantile(dataset_styles[subsample_1m == T]$informative, .01),
  x_max = quantile(dataset_styles[subsample_1m == T]$informative, .99)
)$out %>% cat()

report_clogit(fit14, dataset_styles[subsample_1w == T])

utest(
  fit14,
  dataset_styles[subsample_1w == T],
  "informative",
  x_min = quantile(dataset_styles[subsample_1w == T]$informative, .01),
  x_max = quantile(dataset_styles[subsample_1w == T]$informative, .99)
)$out %>% cat()

# table 8

report_clogit(fit10, dataset_styles[subsample_top == T])

utest(
  fit10,
  dataset_styles[subsample_top == T],
  "informative",
  x_min = quantile(dataset_styles[subsample_top == T]$informative, .01),
  x_max = quantile(dataset_styles[subsample_top == T]$informative, .99)
)$out %>% cat()

# table 9

report_clogit(fit17, dataset_styles[subsample_q75 == T])

utest(
  fit17,
  dataset_styles[subsample_q75 == T],
  "informative",
  x_min = quantile(dataset_styles[subsample_q75 == T]$informative, .01),
  x_max = quantile(dataset_styles[subsample_q75 == T]$informative, .99)
)$out %>% cat()

report_clogit(fit18, dataset_styles[subsample_q50 == T])

utest(
  fit18,
  dataset_styles[subsample_q50 == T],
  "informative",
  x_min = quantile(dataset_styles[subsample_q50 == T]$informative, .01),
  x_max = quantile(dataset_styles[subsample_q50 == T]$informative, .99)
)$out %>% cat()

report_clogit(fit19, dataset_styles[subsample_q25 == T])

utest(
  fit19,
  dataset_styles[subsample_q25 == T],
  "informative",
  x_min = quantile(dataset_styles[subsample_q25 == T]$informative, .01),
  x_max = quantile(dataset_styles[subsample_q25 == T]$informative, .99)
)$out %>% cat()

# table 10

report_clogit(fit7, dataset_styles)

utest(
  fit7,
  dataset_styles,
  "informative",
  x_min = quantile(dataset_styles$informative, .01),
  x_max = quantile(dataset_styles$informative, .99)
)$out %>% cat()

report_clogit(fit8, dataset_styles)

utest(
  fit8,
  dataset_styles,
  "informative",
  x_min = quantile(dataset_styles$informative, .01),
  x_max = quantile(dataset_styles$informative, .99)
)$out %>% cat()

report_clogit(fit9, dataset_styles)

utest(
  fit9,
  dataset_styles,
  "informative",
  x_min = quantile(dataset_styles$informative, .01),
  x_max = quantile(dataset_styles$informative, .99)
)$out %>% cat()

# table 11

report_clogit(fit11, dataset_styles)

utest(
  fit11,
  dataset_styles,
  "informative",
  x_min = quantile(dataset_styles$informative, .01),
  x_max = quantile(dataset_styles$informative, .99)
)$out %>% cat()

# table 12

cbind(descr_reg_genres, cormat_reg_genres[[1]])

# table 13

report_clogit(fit6, dataset_genres)

utest(
  fit6,
  dataset_genres,
  "informative",
  x_min = quantile(dataset_genres$informative, .01),
  x_max = quantile(dataset_genres$informative, .99)
)$out %>% cat()

# appendix f

with(dataset_styles, cor.test(exemplarity, typicality))

dataset_styles[, c(
  "typicality",
  "exemplarity",
  "expertise_style",
  "expertise_genre",
  "informative",
  "distinctive"
)] %>% colldiag()

or(fit15, "exemplarity")
or(fit16, "exemplarity")

# table 14

report_clogit(fit15, dataset_styles[!is.na(exemplarity)])

utest(
  fit15,
  dataset_styles[!is.na(exemplarity)],
  "informative",
  x_min = quantile(dataset_styles[!is.na(exemplarity)]$informative, .01),
  x_max = quantile(dataset_styles[!is.na(exemplarity)]$informative, .99)
)$out %>% cat()

report_clogit(fit16, dataset_styles[!is.na(exemplarity)], fit15)

utest(
  fit16,
  dataset_styles[!is.na(exemplarity)],
  "informative",
  x_min = quantile(dataset_styles[!is.na(exemplarity)]$informative, .01),
  x_max = quantile(dataset_styles[!is.na(exemplarity)]$informative, .99)
)$out %>% cat()
