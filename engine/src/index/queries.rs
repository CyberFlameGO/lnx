use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema};


pub(super) struct NormalQueryFactory {
    parser: QueryParser,
}

impl NormalQueryFactory {
    pub(super) fn create(
        schema: Schema,
        default_search_fields: Vec<String>,
    ) -> Self {

    }
}

pub(super) struct FuzzyQueryFactory {
    prep_fields: Vec<String>,
    search_fields: Vec<(Field, f32)>
}

pub(super) struct MoreLikeThisQueryFactory {

}