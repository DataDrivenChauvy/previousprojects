-- Data Cleaning

-- Performing a simple data cleaning project on a dataset on global companies around Tech employee layoffs during the COVID pandemic during March 2020 to July 2024.
-- Data Source --https://www.kaggle.com/datasets/swaptr/layoffs-2022 

-- Methodology
-- 1. Remove Duplicates
-- 2. Standardise the Data
-- 3. Null Values or blanks values
-- 4. Remove Any Columns 

SELECT* 
FROM world_layoffs.layoffs;

-- 1. Remove Duplicates

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

SELECT * 
FROM world_layoffs.layoffs_staging;

INSERT world_layoffs.layoffs_staging
SELECT* 
FROM world_layoffs.layoffs;

SELECT * 
FROM world_layoffs.layoffs_staging;

SELECT*, 
ROW_NUMBER() OVER(
partition by  industry, total_laid_off, percentage_laid_off,`date`) AS row_num
FROM world_layoffs.layoffs_staging;

SELECT * 
FROM world_layoffs.layoffs_staging;


SELECT *
FROM (
	SELECT company, location, industry, percentage_laid_off, total_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, percentage_laid_off, total_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;


SELECT*
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';

SELECT*
FROM world_layoffs.layoffs_staging
WHERE company = 'Casper';


CREATE TABLE `world_layoffs`.`layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ;

SELECT*
FROM world_layoffs.layoffs_staging2;

INSERT INTO world_layoffs.layoffs_staging2	
	SELECT*,
        ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, percentage_laid_off, total_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
FROM world_layoffs.layoffs_staging;

SELECT*
FROM world_layoffs.layoffs_staging2
WHERE row_num >1 ;


DELETE
FROM world_layoffs.layoffs_staging2
WHERE row_num >1 ;

SELECT*
FROM world_layoffs.layoffs_staging2 ;

-- 2. Standardise the Data

SELECT distinct company, (TRIM(company))
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2
;

SELECT DISTINCT Location
FROM layoffs_staging2
Order by 1
;

SELECT DISTINCT country
FROM layoffs_staging2
Order by 1
;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
Order by 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'UNITED STATES%';

SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- 3 Null & Blank Values

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT DISTINCT industry
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry ='';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry= ''
;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
;

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry  IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL
;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry  IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL
;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry  IS NULL )
AND t2.industry IS NOT NULL
;

SELECT*
FROM layoffs_staging2;

--
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4.REMOVE COLUMN

SELECT*
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT*
FROM world_layoffs.layoffs_staging2;

-- Data Cleaning Complete -- 
-- To recap we did the following:
	-- 1. Remove Duplicates
	-- 2. Standardise the Data
	-- 3. Null Values or blanks values
	-- 4. Remove Any Columns 