package pfsload

import (
	"math/rand"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
)

func FuzzOperation(env *Env, repo, branch, commit string, specs []*OperationSpec) error {
	var totalProb int
	for _, spec := range specs {
		if err := validateProb(spec.Prob); err != nil {
			return err
		}
		totalProb += spec.Prob
	}
	if totalProb != 100 {
		return errors.Errorf("operation probabilities must add up to 100")
	}
	totalProb = 0
	prob := env.Rand().Intn(100)
	for _, spec := range specs {
		totalProb += spec.Prob
		if prob < totalProb {
			return Operation(env, repo, branch, commit, spec)
		}
	}
	panic("should not be able to reach here")
}

func FuzzFile(env *Env, specs []*FileSpec) (*RandomFile, error) {
	var totalProb int
	for _, spec := range specs {
		if err := validateProb(spec.Prob); err != nil {
			return nil, err
		}
		totalProb += spec.Prob
	}
	if totalProb != 100 {
		return nil, errors.Errorf("file probabilities must add up to 100")
	}
	totalProb = 0
	prob := env.Rand().Intn(100)
	for _, spec := range specs {
		totalProb += spec.Prob
		if prob < totalProb {
			return File(env, spec)
		}
	}
	panic("should not be able to reach here")
}

func FuzzSize(specs []*SizeSpec, random *rand.Rand) (*SizeSpec, error) {
	var totalProb int
	for _, spec := range specs {
		if err := validateProb(spec.Prob); err != nil {
			return nil, err
		}
		totalProb += spec.Prob
	}
	if totalProb != 100 {
		return nil, errors.Errorf("size probabilities must add up to 100")
	}
	totalProb = 0
	prob := random.Intn(100)
	for _, spec := range specs {
		totalProb += spec.Prob
		if prob < totalProb {
			return spec, nil
		}
	}
	panic("should not be able to reach here")
}

func shouldExecute(random *rand.Rand, prob int) bool {
	return random.Intn(100) < prob
}

func validateProb(prob int) error {
	if prob < 0 || prob > 100 {
		return errors.Errorf("probabilities must be in the range [0, 100]")
	}
	return nil
}
