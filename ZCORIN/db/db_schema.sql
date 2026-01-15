CREATE TABLE IF NOT EXISTS zcorin_converter (
  id BIGSERIAL PRIMARY KEY,

  material TEXT,
  material_description TEXT,
  country TEXT,
  brand TEXT,
  sub_brand TEXT,
  category TEXT,
  big_category TEXT,
  house TEXT,
  size TEXT,
  pcs_cb TEXT,
  kg_cb TEXT,
  pack_format TEXT,
  size_format TEXT,
  insource_or_outsource TEXT,
  machine_1 TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- biasanya Material itu unik (master data)
CREATE UNIQUE INDEX IF NOT EXISTS ux_zcorin_converter_material
ON zcorin_converter (material);
