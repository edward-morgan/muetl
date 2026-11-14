pub struct Type {
    /// Unique internal specifier for this type.
    pub id: uuid::Uuid,
    /// Human-readable name suitable for error reporting and debugging.
    pub name: String,

    pub underlying_type_id: std::any::TypeId,

    pub validate: Option<Box<dyn Fn(&Option<TypeSchema>, &dyn std::any::Any) -> bool>>,

    pub schema: Option<TypeSchema>,
}

// two kinds of comparison:
// 1. Does Type A contain all fields in Type B such that we can claim a value of A is a value of B?

impl Type {
    pub fn new<T: 'static>(
        name: &str,
        validate: Option<fn(&Option<TypeSchema>, &T) -> bool>,
        schema: Option<TypeSchema>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            underlying_type_id: std::any::TypeId::of::<T>(),
            validate: match validate {
                None => None,
                Some(v) => Some(Box::new(move |schema, instance| {
                    instance.downcast_ref::<T>().is_some_and(|i| v(schema, i))
                })),
            },
            schema,
        }
    }

    /// Define a runtime type that wraps around `T`, providing no additional runtime schema
    /// or validation information.
    pub fn new_wrapper<T: 'static>(name: &str) -> Self {
        Self::new::<T>(name, None, None)
    }
}

#[cfg(test)]
fn testa() {
    let t1 = Type::new::<String>("name", None, None);
}

// Needs:
// 1. Basic abstraction of type name (human readable) vs id
// 2. Support for dynamic runtime types declared
// 3. Runtime checking of dynamic types
// 4. Support for backing runtime types by Rust types
// 5. Support for comparing runtime types during type negotiation
// 6. Support for structural equality

pub struct TypeSchema {
    fields: Vec<SchemaField>,
}

impl TypeSchema {
    pub fn new(fields: Vec<SchemaField>) -> Self {
        Self { fields }
    }

    /// Returns whether the current `TypeSchema` contains at least every field present
    /// in `other` for the purposes of structural equality. This means:
    /// 1. For each field in `other`, `self` contains a field with the same name.
    /// 2. Each pair of fields with the same name have the same `FieldType`.
    pub fn is_superset_of(&self, other: &TypeSchema) -> bool {
        for other_field in &other.fields {
            if let Some(this_field) = self.fields.iter().find(|f| f.name == other_field.name) {
                match (&this_field.field_type, &other_field.field_type) {
                    (FieldType::Builtin(t1), FieldType::Builtin(t2)) if t1 == t2 => {}
                    (FieldType::Object(s1), FieldType::Object(s2)) => {}
                    (_, _) => return false,
                }
            } else {
                return false;
            }
        }

        true
    }
}

pub struct SchemaField {
    pub name: String,
    pub field_type: FieldType,
}

impl SchemaField {
    pub fn new_builtin<T: 'static>(name: &str) -> Self {
        Self::new(name, FieldType::Builtin(std::any::TypeId::of::<T>()))
    }

    pub fn new(name: &str, field_type: FieldType) -> Self {
        Self {
            name: name.to_string(),
            field_type,
        }
    }
}

impl PartialEq for SchemaField {
    fn eq(&self, other: &Self) -> bool {
        if self.name != other.name {
            return false;
        }

        match (&self.field_type, &other.field_type) {
            (FieldType::Builtin(t1), FieldType::Builtin(t2)) if t1 == t2 => true,
            (FieldType::Object(s1), FieldType::Object(s2)) if s1.len() == s2.len() => {
                s1.iter().all(|f1| s2.iter().find(|&f2| f1 == f2).is_some())
            }
            (_, _) => false,
        }
    }
}
pub enum FieldType {
    Builtin(std::any::TypeId),
    Object(Vec<SchemaField>),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_empty_schemas() {
        let schema1 = TypeSchema::new(vec![
            SchemaField::new_builtin::<String>("name"),
            SchemaField::new_builtin::<i32>("age"),
        ]);

        let empty = TypeSchema::new(Vec::new());

        assert!(schema1.is_superset_of(&empty));
    }
}
