-- MySQL dump 10.16  Distrib 10.1.38-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: dev_facebook_test
-- ------------------------------------------------------
-- Server version	10.1.38-MariaDB-0ubuntu0.18.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `adset_conversion_metrics`
--

DROP TABLE IF EXISTS `adset_conversion_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_conversion_metrics` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `add_to_cart` int(11) DEFAULT NULL,
  `initiate_checkout` int(11) DEFAULT NULL,
  `purchase` int(11) DEFAULT NULL,
  `view_content` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `cost_per_purchase` float DEFAULT NULL,
  `cost_per_add_to_cart` float DEFAULT NULL,
  `cost_per_initiate_checkout` float DEFAULT NULL,
  `cost_per_view_content` float DEFAULT NULL,
  `cost_per_landing_page_view` float DEFAULT NULL,
  `cost_per_link_click` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `daily_budget` int(11) DEFAULT NULL,
  `request_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_insights`
--

DROP TABLE IF EXISTS `adset_insights`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_insights` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `target` int(11) DEFAULT NULL,
  `charge` int(11) DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `cost_per_charge` float DEFAULT NULL,
  `bid_amount` int(11) DEFAULT NULL,
  `daily_budget` int(11) DEFAULT NULL,
  `spend` float DEFAULT NULL,
  `request_time` datetime DEFAULT NULL,
  `age_max` int(11) DEFAULT NULL,
  `age_min` int(11) DEFAULT NULL,
  `flexible_spec` blob,
  `geo_locations` blob,
  `reach` int(11) DEFAULT NULL,
  `optimization_goal` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `adset_score`
--

DROP TABLE IF EXISTS `adset_score`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `adset_score` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `adset_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `request_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_conversion_metrics`
--

DROP TABLE IF EXISTS `campaign_conversion_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_conversion_metrics` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `add_to_cart` int(11) DEFAULT NULL,
  `initiate_checkout` int(11) DEFAULT NULL,
  `purchase` int(11) DEFAULT NULL,
  `view_content` int(11) DEFAULT NULL,
  `landing_page_view` int(11) DEFAULT NULL,
  `link_click` int(11) DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `cost_per_purchase` float DEFAULT NULL,
  `cost_per_add_to_cart` float DEFAULT NULL,
  `cost_per_initiate_checkout` float DEFAULT NULL,
  `cost_per_view_content` float DEFAULT NULL,
  `cost_per_landing_page_view` float DEFAULT NULL,
  `cost_per_link_click` float DEFAULT NULL,
  `spend` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `campaign_target`
--

DROP TABLE IF EXISTS `campaign_target`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `campaign_target` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `destination` int(11) DEFAULT NULL,
  `charge_type` varchar(255) DEFAULT NULL,
  `cost_per_target` float DEFAULT NULL,
  `daily_budget` float DEFAULT NULL,
  `daily_charge` float DEFAULT NULL,
  `impressions` int(11) DEFAULT NULL,
  `period` int(11) DEFAULT NULL,
  `spend` int(11) DEFAULT NULL,
  `spend_cap` int(11) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `stop_time` datetime DEFAULT NULL,
  `target` int(11) DEFAULT NULL,
  `target_left` int(11) DEFAULT NULL,
  `target_type` varchar(255) DEFAULT NULL,
  `reach` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `conversion_optimal_weight`
--

DROP TABLE IF EXISTS `conversion_optimal_weight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `conversion_optimal_weight` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `w1` float DEFAULT NULL,
  `w2` float DEFAULT NULL,
  `w3` float DEFAULT NULL,
  `w4` float DEFAULT NULL,
  `w5` float DEFAULT NULL,
  `w6` float DEFAULT NULL,
  `w_spend` float DEFAULT NULL,
  `w_bid` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `default_price`
--

DROP TABLE IF EXISTS `default_price`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `default_price` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `default_price` blob,
  `request_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `optimal_weight`
--

DROP TABLE IF EXISTS `optimal_weight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `optimal_weight` (
  `campaign_id` bigint(20) DEFAULT NULL,
  `weight_kpi` float DEFAULT NULL,
  `weight_spend` float DEFAULT NULL,
  `weight_bid` float DEFAULT NULL,
  `score` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `release_default_price`
--

DROP TABLE IF EXISTS `release_default_price`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `release_default_price` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `default_price` blob,
  `request_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `release_ver_result`
--

DROP TABLE IF EXISTS `release_ver_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `release_ver_result` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `result` blob,
  `request_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `result`
--

DROP TABLE IF EXISTS `result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `result` (
  `campaign_id` bigint(11) DEFAULT NULL,
  `result` blob,
  `request_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-03-12 15:30:59
