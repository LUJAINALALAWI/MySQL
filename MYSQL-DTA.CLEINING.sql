-- Data Cleaning


SELECT *
FROM layoffs;

-- 1.  Remove Duplicates
-- 2. Standardize the Data  
-- 3. Null values or blank values
-- 4. Remove any columns 


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off,'date') AS row_num
FROM layoffs_staging;


WITH dublicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM dublicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';






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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;



INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off,'date', 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


-- Standardizing data


SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

select  DISTINCT industry
FROM layoffs_staging2
order by 1;

select  *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


select  DISTINCT country
FROM layoffs_staging2
order by 1;

Select  DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
order by 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry ='';

SELECT a1.industry, a2.industry
FROM layoffs_staging2 a1
JOIN layoffs_staging2 a2
	on a1.company = a2.company
WHERE (a1.industry IS NULL OR a1.industry ='')
AND a2.industry IS NOT NULL;


UPDATE layoffs_staging2 a1
JOIN layoffs_staging2 a2
	on a1.company = a2.company
SET a1.industry = a2.industry
WHERE a1.industry IS NULL
AND a2.industry IS NOT NULL;

select*
FROM layoffs_staging2
WHERE company = 'Airbnb';

select *
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





























































