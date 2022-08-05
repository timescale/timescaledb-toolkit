---
name: Feature Stabilization
about: Checklist of tasks to move a feature out of experimental
title: ''
labels: ''
assignees: ''

---

## [\<Feature Name>](<link to root issue for feature>)

**What evidence do we have the feature is being used**

**Why do we feel this feature is ready to be stable**

**Is there any known further work needed on this feature after stabilization**

**Are there any compatibility concerns that may arise during future work on this feature**

### Feature History
- Experimental release version:
- Last version modifying on-disk format:
- Target stabilization version:


### Stabilization checklist:
- [ ] Ensure tests exist for all public API
- [ ] Ensure API documentation exists and is accurate
- [ ] Remove `toolkit_experimental` tags and update test usages
- [ ] Add arrow operators for accessors if applicable
- [ ] Ensure arrow operators have test coverage
- [ ] If present, ensure `combine` and `rollup` are tested
- [ ] Add serialization tests for on disk format
- [ ] Add upgrade tests
- [ ] Add continuous aggregate test
- [ ] Add feature level documentation
