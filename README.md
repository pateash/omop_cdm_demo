## Pipelines

1. **L2_omop531-cdm-setup**
Setup vocabulary tables for Common Data Model (CDM)

2. **L6-1_drug-analysis_UsageofOxycodonestratifiedbygender**
This pipeline looks at trends in usage of a given drug over time. Specifically how different population segments are using a given drug. In this example we stratify results by age and gender.

3. **L6-4_Pairwise-Association**
This pipeline examines the association between drugs. For example, in an study of prescription drugs, we might be interested in learning which drugs are more likely prescribed together compared to what would be expected at random. For this analysis, we use two approaches: first we apply a simple probabilistic approach, which uses the cosine measure to quantify the degree of association between a pair of events (in this case, drug1 and drug2 being used during the same drug era). Next, we use the FP-Growth algorithm from spark mlLib to identify frequent patterns for prescription drugs. This is a similar approach to market basket analysis (or affinity analytics).

4. **L6-2_drug_analysis_top-drugs-prescribed-in-the-past-5-years**
Top drugs prescribed in the past five years.

5. **L4-2_omop531-etl-synthea**
Creates CTEs that populate condition_era and drug_era tables

6. **L3-1_omop-vocab-setup_load**
Create vocabulary tables for OMOP5.3.1 Common Data Model (CDM)

Will iteratively ingest CSV files from s3://hls-eng-data-public/data/rwe/omop-vocabs/ and write into 

7. **L4-1_omop531-etl-synthea**
ETL data from source to OMOP model. Creates new tables or inserts data into existing fact tables. 


8. **L3-2_omop-vocab-setup_create_vocab_map_tables**
Create vocabulary mapping tables

9. **L1_data-ingest**
Ingest synthetic data (generated with synthea) into Delta Lake bronze layer. 

This pipeline will iteratively ingest data from multiple CSV source paths and into respective target tables from either a metadata table or dataframe. 




## Datasets

1. **visit_occurence**
Tracks the occurrence of visits made by individuals, including details such as visit start and end dates, visit type, provider, and care site.

2. **observation_period**
Tracks the observation periods for individuals, including the start and end dates of each period and the type of period.

3. **procedure_occurence**
Records of medical procedures performed on individuals, including details such as procedure type, date, quantity, and provider.

4. **person**
Person data containing demographic information such as gender, birth date, race, ethnicity, and location.

5. **drug_exposure**
Records of drug exposures for patients, including details such as start and end dates, dosage, quantity, and provider information.

6. **drug_era**
Tracks the drug exposure periods for individuals, including the start and end dates of each era, the number of drug exposures, and the gap between exposures.

7. **measurement**
Healthcare measurement data capturing various measurements for individuals, including values, units, ranges, and sources.

8. **condition_occurence**
Tracks the occurrence of various medical conditions for individuals, providing insights into the start and end dates, condition types, providers, and visit details.

9. **condition_era**
Tracks the occurrence and duration of medical conditions for individuals, providing insights into the prevalence and frequency of specific conditions.

10. **top_drugs_last_5_years**
Records the top drugs prescribed in the last 5 years, providing insights into drug usage trends.

11. **l0_bronze_layer_immunizations**
Records of immunizations administered to patients at the bronze layer level, including the date, patient information, encounter details, immunization code, description, and cost.

12. **l0_bronze_layer_medications**
Records of medications prescribed to patients in the bronze layer, including details such as start and stop dates, patient information, encounter details, medication code and description, cost, dispenses, total cost, reason code, and reason description.

13. **l0_bronze_layer_procedures**
Records of bronze layer procedures performed on patients, including details such as date, patient information, encounter details, procedure code, description, cost, reason code, and reason description.

14. **l0_bronze_layer_conditions**
Records of bronze layer conditions for patients, including start and stop dates, patient and encounter identifiers, condition codes, and descriptions.

15. **l0_bronze_layer_patients**
Patient data in the bronze layer, containing information such as demographics, contact details, and medical history.

16. **final_visit_ids**
A dataset containing the final visit IDs associated with encounter IDs and new visit occurrence IDs.

17. **assign_all_visit_ids**
Tracks all visit IDs associated with encounters, providing information on the person, dates of service, encounter class, visit type, and visit occurrence.

18. **l0_bronze_layer_observations**
Observations recorded for patients at the bronze layer level, capturing details such as date, patient information, encounter details, code, description, value, units, and type.

19. **all_visits**
Records of patient visits, including encounter details, start and end dates, and visit occurrence identifiers.

20. **l0_bronze_layer_encounters**
Records of encounters in the bronze layer of a healthcare system, capturing details such as patient, provider, encounter class, diagnosis codes, and associated costs.

21. **cohort_attribute**
Cohort attribute data capturing the definition, start and end dates, and attribute values for different subjects.

22. **cohort_definition**
Cohort definitions containing information about the name, description, type, syntax, and initiation date of each cohort.

23. **cohort**
Cohort data containing information about cohort definitions, subject IDs, and the start and end dates of each cohort.

24. **dose_era**
Records the dosage information for individuals, including the drug concept, dose value, start and end dates of the dosage era.

25. **cost**
Cost data related to healthcare services, including charges, payments, and insurance coverage.

26. **payer_plan_period**
Tracks payer plan periods for individuals, including start and end dates, payer and plan details, sponsor information, and stop reasons.

27. **provider**
Provider data containing information such as provider name, National Provider Identifier (NPI), Drug Enforcement Administration (DEA) number, specialty, care site, year of birth, and gender.

28. **care_site**
Dataset containing information about care sites, including their IDs, names, place of service concept IDs, location IDs, and source values. This dataset does not have any associated pipelines.

29. **location**
Location data containing addresses, city, state, zip code, county, and source value.

30. **death**
Records of deaths including person ID, death date, death type, and cause of death.

31. **fact_relationship**
Tracks relationships between different concepts in a fact-based dataset.

32. **specimen**
Specimen data containing information about specimens collected from individuals, including specimen type, date and time of collection, quantity, anatomical site, and disease status.

33. **observation**
Observation data capturing various measurements and attributes related to individuals, such as observation date, value, unit, and source.

34. **note_nlp**
Dataset containing natural language processing (NLP) analysis of notes, capturing concepts, snippets, offsets, and temporal information.

35. **note**
Dataset containing notes related to individuals, including note date, type, title, text, and source.

36. **device_exposure**
Records of device exposures for individuals, capturing details such as device type, start and end dates, quantity, and provider information.

37. **visit_detail**
Records of visit details for individuals, including start and end dates, provider and care site information, and admission and discharge details.

38. **metadata**
Metadata records containing information about concepts, their types, names, values, and dates.

39. **cdm_source**
A dataset containing information about clinical data sources, including their names, abbreviations, holders, descriptions, documentation references, ETL references, release dates, CDM versions, and vocabulary versions.

40. **attribute_definition**
A collection of attribute definitions with their corresponding names, descriptions, types, and syntax. This dataset provides information about the attributes used in a system or application.

41. **drug_strength**
Dataset containing information about drug strengths, including ingredient concept IDs, amount values, unit concept IDs, numerator and denominator values, box size, validity dates, and invalid reasons.

42. **concept_synonym**
A collection of concept synonyms with their corresponding concept IDs and language concept IDs.

43. **relationship**
Dataset containing information about relationships, including relationship IDs, names, hierarchical nature, ancestry definition, reverse relationship IDs, and concept IDs.

44. **concept_class**
A dataset containing concept class information, including unique identifiers and names. This dataset does not have a specific format or any associated pipelines.

45. **domain**
A dataset containing information about domains, including their unique IDs, names, and concept IDs. This dataset does not have a specific format or any associated pipelines.

46. **vocabulary**
A collection of vocabularies with unique identifiers, names, references, versions, and associated concept IDs.

47. **COPRESCRIBED**
Records instances of co-prescribed drugs, including the count and support values for each drug combination.

48. **L0_target**
A dataset containing configured target information.

49. **L0_source**
A list of configured data sources.

50. **L3_target**
A dataset containing target values for a specific task or objective.

51. **L3_source**
A dataset containing information about the source of Level 1 data.

52. **source_to_source_vocab_map**
A mapping table that links source codes to target concepts in different vocabularies.

53. **source_to_standard_vocab_map**
A mapping table that links source codes to standard vocabulary concepts.

54. **source_to_concept_map**
Mapping of source codes to concept IDs in different vocabularies, enabling cross-referencing and integration of data from multiple sources.

55. **concept_ancestor**
A dataset containing information about the hierarchical relationship between concepts. It provides details about the ancestor concept, descendant concept, and the minimum and maximum levels of separation between them.

56. **concept_relationship**
A dataset containing relationships between different concepts, along with their validity dates and reasons for invalidity.

57. **concept**
A collection of concepts with their associated details such as concept name, domain, vocabulary, concept class, and validity dates. This dataset provides a comprehensive reference for various concepts.

58. **l1_silver_omop531_person**
OMOP (Observational Medical Outcomes Partnership) dataset containing demographic information of individuals, including gender, birth date, race, and ethnicity.# omop_cdm_demo
