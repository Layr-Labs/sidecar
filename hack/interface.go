package hack

type SomeBase struct {
}

func (s *SomeBase) SomeMethod() {

}

type MyCoolInterface interface {
	SomeMethod()
}

type MyCoolStruct struct {
	SomeBase
}

func NewMyCoolStruct() MyCoolInterface {
	return &MyCoolStruct{}
}
