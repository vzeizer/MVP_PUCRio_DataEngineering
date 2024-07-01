------------------------------------------------------------------------------------------
----------------------------------SQL Queries --------------------------------------------
------------------------------------------------------------------------------------------

-- Number of Cities in the State of Paraná, which is equal to 400 - 1 = 399 
-- (excluding the State of Paraná).

SELECT COUNT(DISTINCT(localidade)) AS cont_localidade FROM `CrimesPR_csv`

-- Q2. Top 10 cities in State of Paraná with the highest "Rape Crimes", along with 
-- their Minimum, Maximum and Standard Deviation.

SELECT localidade, AVG(crimes_de_estupro) as avg_estupro, MIN(crimes_de_estupro) as min_estupro,
MAX(crimes_de_estupro) as max_estupro, STD(crimes_de_estupro) as std_estupro
FROM `CrimesPR_csv` 
GROUP BY(localidade) ORDER BY(AVG(crimes_de_estupro) ) DESC LIMIT 10


-- Q3. Top 10 cities in State of Paraná with the highest "Robbery Crimes", along with
-- their Minimum, Maximum and Standard Deviation.

SELECT localidade, AVG(crimes_de_furto) as avg_furto, MIN(crimes_de_furto) as min_furto,
MAX(crimes_de_furto) as max_furto, STD(crimes_de_furto) as std_furto 
FROM `CrimesPR_csv` 
GROUP BY(localidade) ORDER BY(AVG(crimes_de_furto)) DESC LIMIT 10 

-- Q4. Top 10 cities in State of Paraná with the highest "Drug Trafficking Occurrences", 
-- along with their Minimum, Maximum and Standard Deviation.

SELECT localidade, AVG(ocorrencias_envolvendo_trafico_de_drogas) as avg_trafico,
MIN(ocorrencias_envolvendo_trafico_de_drogas) as min_trafico,
MAX(ocorrencias_envolvendo_trafico_de_drogas) as max_trafico,
STD(ocorrencias_envolvendo_trafico_de_drogas) as std_trafico 
FROM `CrimesPR_csv` GROUP BY(localidade) ORDER BY(AVG(ocorrencias_envolvendo_trafico_de_drogas)) 
DESC LIMIT 10 

-- Q5. Top 10 cities in State of Paraná with the highest "Vehicle Thefts", along with
-- their Minimum, Maximum and Standard Deviation.

SELECT localidade, AVG(furtos_de_veiculos) as avg_furtos_veiculos, 
MIN(furtos_de_veiculos) as min_furtos_veiculos,
MAX(furtos_de_veiculos) as max_furtos_veiculos, STD(furtos_de_veiculos) as std_furtos_veiculos 
FROM `CrimesPR_csv` 
GROUP BY(localidade) ORDER BY(AVG(furtos_de_veiculos) ) DESC LIMIT 10

-- Q6. Top 10 cities in State of Paraná with the highest "Disturbing the Piece/Tranquility", 
-- along with their Minimum, Maximum and Standard Deviation.

SELECT localidade, AVG(perturbacao_do_sossegotranquilidade) as avg_sossego_tranq, 
MIN(perturbacao_do_sossegotranquilidade) as min_sossego_tranq,
MAX(perturbacao_do_sossegotranquilidade) as max_sossego_tranq, 
STD(perturbacao_do_sossegotranquilidade) as std_sossego_tranq
FROM `CrimesPR_csv` 
GROUP BY(localidade) ORDER BY(AVG(perturbacao_do_sossegotranquilidade) ) DESC LIMIT 10

-- Q7. What is the mean Drug Trafficking and Drug use Occurrences in Curitiba? 

SELECT localidade, AVG(ocorrencias_envolvendo_trafico_de_drogas) as avg_trafico_curitiba,
       AVG(ocorrencias_envolvendo_usoconsumo_de_drogas) as avg_druguse_curitiba
FROM `CrimesPR_csv` 
GROUP BY localidade 
HAVING localidade = 'Curitiba'

-- Q8. Concerning the last question, what about specifically in year 2020?

SELECT localidade, ocorrencias_envolvendo_trafico_de_drogas as trafico_curitiba,
                  ocorrencias_envolvendo_usoconsumo_de_drogas as druguse_curitiba
FROM `CrimesPR_csv` 
WHERE localidade = 'Curitiba' AND ano = 2020


--------------------------------------------------------------
--------- Data Quality Issues Investigations -----------------
--------------------------------------------------------------


-- 1. Next, it is going to be addressed descriptive statistics about Rape Crimes in 
-- **Curitiba** in order to find inconsistencies in the data.

SELECT localidade, AVG(crimes_de_estupro) as avg_estupro, 
MIN(crimes_de_estupro) as min_estupro_curitiba,
MAX(crimes_de_estupro) as max_estupro_curitiba
FROM `CrimesPR_csv` 
GROUP BY(localidade) HAVING localidade = 'Curitiba'

-- 2. Next, it is going to be addressed descriptive statistics about Drug Trafficking
-- Occurrences in **Curitiba** in order to find inconsistencies in the data.

SELECT localidade, AVG(ocorrencias_envolvendo_trafico_de_drogas) as avg_traffic_curitiba, 
MIN(ocorrencias_envolvendo_trafico_de_drogas) as min_traffic_curitiba,
MAX(ocorrencias_envolvendo_trafico_de_drogas) as max_traffic_curitiba
FROM `CrimesPR_csv` 
GROUP BY(localidade) HAVING localidade = 'Curitiba'

-- 3. Next, it is going to be addressed descriptive statistics about Bodily Injuries 
-- in **Curitiba** in order to find inconsistencies in the data.

SELECT localidade, AVG(crimes_de_lesao_corporal) as avg_lesion_curitiba, 
MIN(crimes_de_lesao_corporal) as min_lesion_curitiba,
MAX(crimes_de_lesao_corporal) as max_lesion_curitiba
FROM `CrimesPR_csv` 
GROUP BY(localidade) HAVING localidade = 'Curitiba'

-- 4. Next, it is going to be addressed descriptive statistics about Bodily Injuries 
-- in **São José dos Pinhais** in order to find inconsistencies in the data.

SELECT localidade, AVG(crimes_de_lesao_corporal) as avg_lesion_SJP, 
MIN(crimes_de_lesao_corporal) as min_lesion_SJP,
MAX(crimes_de_lesao_corporal) as max_lesion_SJP
FROM `CrimesPR_csv` 
GROUP BY(localidade) HAVING localidade = 'São José dos Pinhais'

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------