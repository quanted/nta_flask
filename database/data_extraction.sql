select c_suc.dsstox_compound_id as msr_dsstox_compound_id, c_suc.monoisotopic_mass as msr_monoisotopic_mass, c_suc.smiles as msr_smiles , c_suc.mol_formula as msr_mol_formula,
gs.dsstox_substance_id, gs.preferred_name, gs.casrn, c.jchem_inchi_key, c.acd_iupac_name, c.mol_formula,
c.monoisotopic_mass, em.Total_median as total_median,
CASE
WHEN em.Total_median IS NULL
THEN ''
ELSE concat('https://comptox.epa.gov/dashboard/dsstoxdb/results?search=', gs.dsstox_substance_id, '#exposure-predictions')
END expocast_comptox_link,
CASE em.inNHANES
WHEN 'TRUE'
THEN concat('https://comptox.epa.gov/dashboard/dsstoxdb/results?search=', gs.dsstox_substance_id,'#monitoring-data')
ELSE ''
END nhanes_comptox_link,
cac.assay_count_active, cac.assay_count_total,
SOURCE_COUNT.DATA_SOURCES as data_sources
FROM compounds c_suc
JOIN compound_relationships as cr ON cr.fk_compound_id_successor = c_suc.id
JOIN compounds as c ON cr.fk_compound_id_predecessor = c.id
JOIN generic_substance_compounds as gsc ON c.id = gsc.fk_compound_id
JOIN generic_substances as gs ON gsc.fk_generic_substance_id = gs.id
JOIN (
	Select gs.id, count(DISTINCT ss.fk_chemical_list_id)+1 as DATA_SOURCES
	from generic_substances gs
	LEFT JOIN source_generic_substance_mappings as sgsm ON sgsm.fk_generic_substance_id = gs.id
	LEFT JOIN source_substances as ss ON ss.id = sgsm.fk_source_substance_id
	group by gs.id
) SOURCE_COUNT on gs.id = SOURCE_COUNT.id
LEFT JOIN prod_invitro.chemical_assay_count as cac ON cac.chid = gs.id
LEFT JOIN prod_icss.expocast_models as em ON em.casrn = gs.casrn
WHERE cr.fk_compound_relationship_type_id = 2 AND c.has_defined_isotope = 0;