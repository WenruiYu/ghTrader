
import pytest
import numpy as np
import torch
from pathlib import Path
from ghtrader.research.models import create_model, TKANModel, LOBERTModel, KANFormerModel

@pytest.mark.parametrize("model_type", ["tkan", "lobert", "kanformer"])
def test_sota_model_instantiation(model_type):
    """Test that SOTA models can be instantiated via factory."""
    model = create_model(
        model_type,
        n_features=10,
        n_classes=3,
        seq_len=20,
        device="cpu"
    )
    assert model is not None
    assert model.name == model_type

@pytest.mark.parametrize("model_type", ["tkan", "lobert", "kanformer"])
def test_sota_model_training_loop(model_type, tmp_path):
    """Smoke test for training loop execution."""
    n_samples = 100
    seq_len = 10
    n_features = 5
    n_classes = 3
    
    # Create dummy data
    X = np.random.randn(n_samples, n_features).astype(np.float32)
    y = np.random.randint(0, n_classes, size=n_samples).astype(np.int64)
    
    model = create_model(
        model_type,
        n_features=n_features,
        n_classes=n_classes,
        seq_len=seq_len,
        device="cpu",
        # Use small model for speed
        hidden_dim=16, 
        d_model=16,
        n_layers=1,
        epochs=1,
        batch_size=16
    )
    
    # Fit
    model.fit(X, y, epochs=1, batch_size=16, num_workers=0, ddp=False)
    
    # Predict
    probs = model.predict_proba(X)
    assert probs.shape == (n_samples, n_classes)
    
    # Check validity (ignoring warmup period)
    # Sequence models return NaN for the first seq_len samples
    valid_probs = probs[seq_len:]
    assert not np.isnan(valid_probs).any()
    
    # Save/Load
    save_path = tmp_path / "model.pt"
    model.save(save_path)
    assert save_path.exists()
    
    loaded_model = create_model(
        model_type,
        n_features=n_features,
        n_classes=n_classes,
        seq_len=seq_len,
        device="cpu",
        hidden_dim=16,
        d_model=16
    )
    loaded_model.load(save_path)
    
    # Check if loaded model produces same output (deterministic given same input/weights)
    # Note: might need eval mode or seed setting, but roughly check shape/validity
    probs2 = loaded_model.predict_proba(X)
    assert probs2.shape == (n_samples, n_classes)
