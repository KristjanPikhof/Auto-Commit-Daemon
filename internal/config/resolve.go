package config

// ResolveAll resolves the supported settings in catalog order.
func ResolveAll(input ResolveInput) (map[string]ResolvedField, error) {
	out := make(map[string]ResolvedField, len(fieldCatalog))
	for _, field := range fieldCatalog {
		value, err := ResolveField(field.Name, input)
		if err != nil {
			return nil, err
		}
		out[field.Name] = value
	}
	return out, nil
}
