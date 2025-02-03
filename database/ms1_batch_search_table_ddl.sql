CREATE SEQUENCE ms1_batch_search_seq;

CREATE TABLE ms1_batch_search (
  id INT NOT NULL DEFAULT NEXTVAL ('ms1_batch_search_seq'),
  msr_dsstox_compound_id VARCHAR(45) NULL,
  msr_monoisotopic_mass DOUBLE PRECISION NULL,
  msr_smiles TEXT NULL,
  msr_mol_formula VARCHAR(255) NULL,
  dsstox_substance_id VARCHAR(45) NULL,
  preferred_name TEXT NULL,
  casrn VARCHAR(45) NULL,
  jchem_inchi_key VARCHAR(45) NULL,
  acd_iupac_name TEXT NULL,
  mol_formula VARCHAR(255) NULL,
  monoisotopic_mass DOUBLE PRECISION NULL,
  total_median VARCHAR(255) NULL,
  expocast_comptox_link VARCHAR(1024) NULL,
  nhanes_comptox_link VARCHAR(1024) NULL,
  assay_count_active INT NULL,
  assay_count_total INT NULL,
  data_sources INT NULL,
  PRIMARY KEY (id));

  CREATE INDEX on ms1_batch_search (msr_mol_formula);
  CREATE INDEX on ms1_batch_search (msr_monoisotopic_mass);
  CREATE INDEX on ms1_batch_search (mol_formula);
  CREATE INDEX on ms1_batch_search (monoisotopic_mass);
