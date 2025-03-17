-- ---------------------------------------------------------DATA CLEANING --------------------------------------------------
SELECT * FROM LAYOFFS;

-- CREATE TABLE FROM EXCISTING TABLE (COPY OF THE TABLE)
CREATE TABLE LAYOFFS_STAGING LIKE LAYOFFS;
SELECT * FROM LAYOFFS_STAGING;

-- INSERT THE VALUE OF THE EXCISTING TABLE
INSERT LAYOFFS_STAGING
SELECT * FROM LAYOFFS;

-- FIND DUPLICATES
SELECT *, ROW_NUMBER() OVER (
PARTITION BY COMPANY,LOCATION,INDUSTRY,`DATE`,STAGE,COUNTRY,TOTAL_LAID_OFF) 
as row_no from layoffs_staging;

WITH DUPLICATES_CTE as 
(
SELECT *,
 ROW_NUMBER() OVER (
PARTITION BY COMPANY,LOCATION,INDUSTRY,`DATE`,STAGE,COUNTRY,TOTAL_LAID_OFF,funds_raised_millions) as row_no 
from layoffs_staging
)
select * 
from duplicates_cte 
WHERE ROW_NO >1;
select * from layoffs_staging where company = 'casper';

WITH DUPLICATES_CTE as 
(
SELECT *,
 ROW_NUMBER() OVER (
PARTITION BY COMPANY,LOCATION,INDUSTRY,`DATE`,STAGE,COUNTRY,TOTAL_LAID_OFF,funds_raised_millions) as row_no 
from layoffs_staging
)
delete 
from duplicates_cte 
WHERE ROW_NO >1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_no int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select * from layoffs_staging2;
insert into layoffs_staging2  
SELECT *,
 ROW_NUMBER() OVER (
PARTITION BY COMPANY,LOCATION,INDUSTRY,`DATE`,STAGE,COUNTRY,TOTAL_LAID_OFF,funds_raised_millions) as row_no 
from layoffs_staging;
SET SQL_SAFE_UPDATES = 0;
delete from layoffs_staging2 where row_no>1;
select * from layoffs_staging2 where row_no>1 ;

update layoffs_staging2 set company = trim(company) ;

select industry from layoffs_staging2 where  industry like 'Crypto%';
update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';

select distinct country ,trim(trailing '.' from country) from layoffs_staging2;
update layoffs_staging2 set country = trim(trailing '.' from country) where country like 'United States%';

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2 set `date`= str_to_date(`date`,'%m/%d/%Y');
alter table layoffs_staging2 modify column `date` DATE;

update layoffs_staging2 
set industry = null
where industry = '';

select  * from layoffs_staging2 where company='Airbnb';

update layoffs_staging2 t1 join layoffs_staging2 t2 
on t1.company=t2.company 
set t1.industry =t2.industry where t1.industry is null 
and t2.industry is not null;

select t1.industry,t2.industry from layoffs_staging2 t1
join  layoffs_staging2 t2 on t1.company = t2.company 
where (t1.industry is null or t1.industry ='' )
and  t2.industry is not null;

select  * from layoffs_staging2 where company='Airbnb';

select  * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null ;
delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null ;

alter table layoffs_staging2 drop column row_no;

select  * from layoffs_staging2;

--  ---------------------------------------------------------- EXPLORATORY DATA ANALYSIS-----------------------------------

select  * from layoffs_staging2;

-- Max and Min percentage of total laid off
SELECT MAX(percentage_laid_off),min(percentage_laid_off) FROM layoffs_staging2
where percentage_laid_off is not null;

-- which company had 100(1) percentage of laid off
SELECT * 
FROM layoffs_staging2 
WHERE percentage_laid_off =1 order by total_laid_off desc;

-- Companies with the biggest single Layoff
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

select min(`date`),max(`date`) from layoffs_staging2;
-- Companies with the most Total Layoffs
SELECT company,SUM(TOTAL_LAID_OFF) as total FROM layoffs_staging2 group by company order by 2 desc;
-- By Industry
SELECT industry,SUM(TOTAL_LAID_OFF) as total FROM layoffs_staging2 group by industry order by 2 desc;
-- By Country
SELECT country,SUM(TOTAL_LAID_OFF) as total FROM layoffs_staging2 group by country order by 2 desc;
-- Year wise total laid off
SELECT year(`date`),SUM(TOTAL_LAID_OFF) as total FROM layoffs_staging2 group by year(`date`) order by 1 desc;
-- By Stage
SELECT stage,SUM(TOTAL_LAID_OFF) as total FROM layoffs_staging2 group by stage order by 2 desc;


-- Running Total of Layoffs Per Month
with running_total as(
select substring(`date`,1,7) as `Month` ,sum(total_laid_off) as total_off from layoffs_staging2
where substring(`date`,1,7) is not null group by `Month` 
order by 1 asc
)
select `Month`,total_off,sum(total_off)  over(order by `month`) from running_total ;


-- Companies with the most Layoffs per year
with company_year(company,years,total_laid_off) as(
select company,year(`date`),sum(total_laid_off) from layoffs_staging2 
group by company,year(`date`)
), company_year_ranking as (select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year where years is not null
)
select * from company_year_ranking where ranking <=5 ;