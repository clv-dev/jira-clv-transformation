## PULL REQUEST TEMPLATE

### Description

Describe purpose and provide context for PR. Also add URLs to projects, models, views, explores etc.

### Type of change

Please delete options that are not relevant.

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)

### Testing

Explain how and where testing was conducted (Running in Preview, checking syntax)

### Pull Request Checklist
#### Fill out sections above and mark them off on the checklist below before requesting a code review

* [ ] Naming convention as per showed in data model
* [ ] Transform data type for each column
* [ ] Layer CTEs for different functionality
    * [ ] source
    * [ ] rename_column
    * [ ] cast_type
    * [ ] handle_null
    * [ ] add_undefined_record
    * [ ] calculate_measure
* [ ] Always use LEFT JOIN