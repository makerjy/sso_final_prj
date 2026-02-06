# MIMIC DDL 요약

**요약**

이 문서는 `db/ddl/mimic_ddl.ddl`에 정의된 테이블, 인덱스, 제약조건을 한눈에 확인할 수 있도록 정리한 목록입니다. 스키마는 모두 `sso`이며, 본 문서에는 `GRANT`, `TABLESPACE`, `STORAGE` 같은 물리 설정은 포함하지 않습니다.

- Source: `db/ddl/mimic_ddl.ddl`
- 테이블: 24
- 인덱스: 8 (모두 UNIQUE)
- 제약조건(명명된 것): 19 (PK 8, FK 11)


**Tablespace / Datafile**

| Item | Value |
| --- | --- |
| TABLESPACE_NAME | TEAM9_TBS |
| FILE_NAME | +DATA/KDT2_DB/468873D0155E58CDE0632100000A59DE/DATAFILE/team9_tbs.291.1221402543 |
| FILE# | 23 |
| BYTES | 118111600640 |
| BLOCKS | 14417920 |
| STATUS | AVAILABLE |
| BLOCK_SIZE | 1024 |
| AUTOEXTENSIBLE | YES |
| MAXBYTES | 118111600640 |
| MAXBLOCKS | 14417920 |
| INCREMENT_BY | 131072 |
| USER_BYTES | 118040395776 |
| USER_BLOCKS | 14409228 |
| ONLINE_STATUS | ONLINE |
| FROZEN | OFF |\n**설명**

- `PK`는 각 테이블의 기본키 제약조건이며, 동일 이름의 `UNIQUE` 인덱스를 사용하도록 정의되어 있습니다.
- `FK`는 환자(`sso.patients`) 및 입원(`sso.admissions`)을 기준으로 주요 이벤트/진단/처방 테이블이 참조하는 구조입니다.
- `fk_emar_subject`는 `NOVALIDATE`로 선언되어 있어 기존 데이터에 대한 검증을 생략합니다.

**Tables**

| Table | Schema |
| --- | --- |
| admissions | sso |
| caregiver | sso |
| chartevents | sso |
| d_icd_diagnoses | sso |
| d_icd_procedures | sso |
| d_items | sso |
| d_labitems | sso |
| datetimeevents | sso |
| diagnoses_icd | sso |
| emar | sso |
| emar_detail | sso |
| icustays | sso |
| ingredientevents | sso |
| inputevents | sso |
| labevents | sso |
| microbiologyevents | sso |
| outputevents | sso |
| patients | sso |
| poe | sso |
| prescriptions | sso |
| procedureevents | sso |
| procedures_icd | sso |
| services | sso |
| transfers | sso |

**Indexes**

| Index | Table | Columns | Unique |
| --- | --- | --- | --- |
| pk_admissions | sso.admissions | hadm_id | YES |
| pk_d_icd_diagnoses | sso.d_icd_diagnoses | icd_code, icd_version | YES |
| pk_d_icd_procedures | sso.d_icd_procedures | icd_code, icd_version | YES |
| pk_emar | sso.emar | emar_id, emar_seq | YES |
| pk_icustays | sso.icustays | stay_id | YES |
| pk_labevents | sso.labevents | labevent_id | YES |
| pk_patients | sso.patients | subject_id | YES |
| pk_transfers | sso.transfers | transfer_id | YES |

**Constraints - Primary Keys**

| Constraint | Table | Columns |
| --- | --- | --- |
| pk_admissions | sso.admissions | hadm_id |
| pk_d_icd_diagnoses | sso.d_icd_diagnoses | icd_code, icd_version |
| pk_d_icd_procedures | sso.d_icd_procedures | icd_code, icd_version |
| pk_emar | sso.emar | emar_id, emar_seq |
| pk_icustays | sso.icustays | stay_id |
| pk_labevents | sso.labevents | labevent_id |
| pk_patients | sso.patients | subject_id |
| pk_transfers | sso.transfers | transfer_id |

**Constraints - Foreign Keys**

| Constraint | From | To | Notes |
| --- | --- | --- | --- |
| fk_adm_subject | sso.admissions.subject_id | sso.patients.subject_id |  |
| fk_diag_hadm | sso.diagnoses_icd.hadm_id | sso.admissions.hadm_id |  |
| fk_diag_icd | sso.diagnoses_icd.(icd_code, icd_version) | sso.d_icd_diagnoses.(icd_code, icd_version) |  |
| fk_diag_subject | sso.diagnoses_icd.subject_id | sso.patients.subject_id |  |
| fk_emar_subject | sso.emar.subject_id | sso.patients.subject_id | NOVALIDATE |
| fk_icu_hadm | sso.icustays.hadm_id | sso.admissions.hadm_id |  |
| fk_icu_subject | sso.icustays.subject_id | sso.patients.subject_id |  |
| fk_labs_subject | sso.labevents.subject_id | sso.patients.subject_id |  |
| fk_presc_subject | sso.prescriptions.subject_id | sso.patients.subject_id |  |
| fk_proc_subject | sso.procedures_icd.subject_id | sso.patients.subject_id |  |
| fk_transfers_subject | sso.transfers.subject_id | sso.patients.subject_id |  |
