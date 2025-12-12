# Contributing to Data & AI Pipeline

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers and help them get started
- Focus on constructive feedback
- Respect different viewpoints and experiences

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/yourusername/data-ai-pipeline/issues)
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, Spark version)
   - Relevant logs or error messages

### Suggesting Enhancements

1. Check existing [Issues](https://github.com/yourusername/data-ai-pipeline/issues) for similar suggestions
2. Create a new issue describing:
   - The enhancement and its benefits
   - Possible implementation approach
   - Any potential drawbacks

### Pull Requests

1. **Fork the repository**
   ```bash
   git clone https://github.com/yourusername/data-ai-pipeline.git
   cd data-ai-pipeline
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**
   - Write clean, readable code
   - Follow PEP 8 style guide
   - Add docstrings to functions and classes
   - Update documentation as needed

4. **Add tests**
   ```bash
   # Add tests in tests/ directory
   pytest tests/test_your_feature.py
   ```

5. **Run all tests**
   ```bash
   pytest
   black src/ tests/
   flake8 src/ tests/
   ```

6. **Commit your changes**
   ```bash
   git add .
   git commit -m "Add feature: description of your changes"
   ```

7. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

8. **Create a Pull Request**
   - Go to the original repository
   - Click "New Pull Request"
   - Select your feature branch
   - Provide a clear description of your changes

## Development Setup

### Prerequisites

- Python 3.9+
- Apache Spark 3.3+
- Docker (optional)

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/yourusername/data-ai-pipeline.git
cd data-ai-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e ".[dev]"

# Install pre-commit hooks
pip install pre-commit
pre-commit install
```

### Running Tests

```bash
# All tests
pytest

# Specific test file
pytest tests/test_entity_resolution.py

# With coverage
pytest --cov=src --cov-report=html

# Unit tests only
pytest -m unit

# Integration tests only
pytest -m integration
```

### Code Style

We use:
- **Black** for code formatting
- **Flake8** for linting
- **Pylint** for additional code quality checks
- **MyPy** for type checking (optional)

```bash
# Format code
black src/ tests/

# Check linting
flake8 src/ tests/

# Run pylint
pylint src/
```

## Project Structure

```
src/
├── ingestion/       # Data ingestion logic
├── deduplication/   # Entity resolution
├── iceberg/         # Iceberg table management
├── ml/              # ML training pipeline
└── utils/           # Utility functions

tests/               # Test files mirror src/ structure
```

## Adding New Features

### 1. Data Ingestion Module

If adding a new data source:

```python
# src/ingestion/new_source_ingestor.py
class NewSourceIngestor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def read_data(self) -> DataFrame:
        # Implementation
        pass
```

### 2. Entity Resolution

If improving matching algorithms:

```python
# src/deduplication/advanced_matcher.py
def advanced_similarity(name1: str, name2: str) -> float:
    # Implementation
    pass
```

### 3. ML Models

If adding new models:

```python
# src/ml/new_model_trainer.py
class NewModelTrainer:
    def train(self, df: DataFrame):
        # Implementation
        pass
```

## Documentation

- Update README.md for user-facing changes
- Add docstrings to all functions and classes
- Update inline comments for complex logic
- Update configuration examples if needed

### Docstring Format

```python
def function_name(param1: str, param2: int) -> bool:
    """
    Brief description of what the function does.
    
    Args:
        param1: Description of param1
        param2: Description of param2
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: Description of when this is raised
    """
    pass
```

## Testing Guidelines

### Unit Tests

- Test individual functions in isolation
- Mock external dependencies (Spark, AWS services)
- Use pytest fixtures for reusable test data

```python
def test_normalize_name():
    assert normalize_name("Acme Corp") == "acme"
```

### Integration Tests

- Test component interactions
- Use real Spark sessions (local mode)
- Mark with `@pytest.mark.integration`

```python
@pytest.mark.integration
def test_full_pipeline(spark_session):
    # Test multiple components together
    pass
```

## Review Process

1. Maintainers will review your PR
2. Address any requested changes
3. Once approved, your PR will be merged
4. Your contribution will be acknowledged in release notes

## Questions?

- Open an issue for questions
- Join our discussions
- Email: support@example.com

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing! 🎉
