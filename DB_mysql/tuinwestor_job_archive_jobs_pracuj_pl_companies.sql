-- MySQL dump 10.13  Distrib 8.0.25, for Win64 (x86_64)
--
-- Host: 51.38.128.241    Database: tuinwestor
-- ------------------------------------------------------
-- Server version	8.0.29-0ubuntu0.20.04.3

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `job_archive_jobs_pracuj_pl_companies`
--

DROP TABLE IF EXISTS `job_archive_jobs_pracuj_pl_companies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `job_archive_jobs_pracuj_pl_companies` (
  `id` int NOT NULL AUTO_INCREMENT,
  `job_offer_id` int DEFAULT NULL,
  `comp_id` int DEFAULT NULL,
  `company_name` varchar(120) DEFAULT NULL,
  `company_krs` varchar(11) DEFAULT NULL,
  `if_subsidiary` tinyint NOT NULL,
  `subsidiary_name` varchar(120) DEFAULT NULL,
  `subsidiary_krs` varchar(11) DEFAULT NULL,
  `job_id` int DEFAULT NULL,
  `job_title` text,
  `job_company` text,
  `cleaned_string` varchar(120) DEFAULT NULL,
  `job_locs` text,
  `job_link` text,
  `date_published` date DEFAULT NULL,
  `page` int DEFAULT NULL,
  `year_month_str` varchar(9) DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `ind_date_published` (`date_published`),
  KEY `ind_year_month_str` (`year_month_str`),
  KEY `ind_job_id` (`job_id`)
) ENGINE=InnoDB AUTO_INCREMENT=222600 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-07-20 13:32:38
