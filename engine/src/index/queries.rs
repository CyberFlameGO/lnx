use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, FieldType};
use tantivy::tokenizer::TokenizerManager;
use tantivy::{Score, Index};

use serde::{Serialize, Deserialize};
use hashbrown::HashMap;

use crate::structures::LoadedIndex;
use crate::helpers::hash;
use crate::correction;

#[inline(always)]
fn add_field_if_valid(pair: (Field, f32), valid_fields: &mut Vec<(Field, f32)>, field_type: &FieldType) {
    if let FieldType::Str(_) = field_type {
        valid_fields.push(pair);
    }
}

/// Query context for the relevant factories.
#[derive(Serialize, Deserialize)]
pub struct QueryContext {
    /// The fields actually searched as part of the queries.
    ///
    /// For fuzzy queries only the TEXT fields are used.
    search_fields: Vec<String>,

    /// Fields used for altering the bias to fields.
    #[serde(default)]
    boost_fields: HashMap<String, Score>,

    /// Sets the default mode for the query parser, if set to `true` this is
    /// AND otherwise OR.
    #[serde(default)]
    set_conjunction_by_default: bool,

    /// If to use the fast-fuzzy system this will need to be `true` to be enabled.
    #[serde(default)]
    use_fast_fuzzy: bool,

    /// If enabled stop words will be stripped from the query (fuzzy only)
    #[serde(default)]
    strip_stop_words: bool,
}

pub(super) struct QueryHandler {
    normal_factory: NormalQueryFactory,
    fuzzy_factory: FuzzyQueryFactory,
    more_like_this_factory: MoreLikeThisQueryFactory,
}

impl QueryHandler {
    fn create(
        index: Index,
        query_context: QueryContext,
    ) -> Self {
        let schema = index.schema();
        let mut query_parser_search_fields = (vec![], vec![]);
        let mut fuzzy_query_search_fields = vec![];

        // We need to extract out the fields from name to id.
        for ref_field in query_context.search_fields {
            let pre_processed_field = format!("_{}", hash(&ref_field));

            // This checks if a search field is a indexed text field (it has a private field)
            // that's used internally, since we pre-compute the correction behaviour before
            // hand, we want to actually target those fields not the inputted fields.
            match (
                schema.get_field(&ref_field),
                schema.get_field(&pre_processed_field),
            ) {
                (Some(standard), Some(pre_processed)) => {
                    let boost = if let Some(boost) = loader.boost_fields.get(&ref_field) {
                        debug!("boosting field for query parser {} {}", &ref_field, boost);
                        *boost
                    } else {
                        0f32
                    };

                    if loader.use_fast_fuzzy && correction::enabled() {
                        query_parser_search_fields.0.push(pre_processed);
                        query_parser_search_fields.1.push(boost);

                        let field_type = schema.get_field_entry(pre_processed);
                        add_field_if_valid(
                            (pre_processed, boost),
                            &mut fuzzy_query_search_fields,
                            field_type.field_type(),
                        );
                    } else {
                        query_parser_search_fields.0.push(standard);
                        query_parser_search_fields.1.push(boost);

                        let field_type = schema.get_field_entry(standard);
                        add_field_if_valid(
                            (pre_processed, boost),
                            &mut fuzzy_query_search_fields,
                            field_type.field_type(),
                        );
                    }
                },
                (Some(field), None) => {
                    let boost = if let Some(boost) = loader.boost_fields.get(&ref_field) {
                        debug!("boosting field for query parser {} {}", &ref_field, boost);
                        *boost
                    } else {
                         0.0f32
                    };

                    query_parser_search_fields.0.push(field);
                    query_parser_search_fields.1.push(boost);

                    let field_type = schema_copy.get_field_entry(field);
                    add_field_if_valid(
                        (field, boost),
                        &mut fuzzy_query_search_fields,
                        field_type.field_type(),
                    );
                },
                (None, _) => {
                    let fields: Vec<String> = schema_copy
                        .fields()
                        .map(|(_, v)| v.name().to_string())
                        .collect();

                    return Err(Error::msg(format!(
                        "you defined the schema with the following fields: {:?} \
                        and declared the a search_field {:?} but this does not exist in the defined fields.",
                        fields, &ref_field
                    )));
                },
            };
        }


        let f_qf = FuzzyQueryFactory::create();
        let mlt_qf = MoreLikeThisQueryFactory::create();
        let n_qf = NormalQueryFactory::create();


    }
}

struct NormalQueryFactory {
    parser: QueryParser,
}

impl NormalQueryFactory {
    fn create(
        index: &Index,
        default_fields: Vec<Field>,
    ) -> Self {
        let parser = QueryParser::for_index(
            index,
            default_fields,
        );

        Self {
            parser
        }
    }
}

struct FuzzyQueryFactory {
    prep_fields: Vec<String>,
    search_fields: Vec<(Field, Score)>
}

impl FuzzyQueryFactory {
    fn create() -> Self {
        Self {
            prep_fields: vec![],
            search_fields: vec![],
        }
    }
}

struct MoreLikeThisQueryFactory {}

impl MoreLikeThisQueryFactory {
    fn create() -> Self {
        Self {}
    }
}


