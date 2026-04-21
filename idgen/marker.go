package idgen

type Marker interface {
	Strategy() Strategy
	UUIDVersion() string
}

type IDAuto struct{}
type IDSnowflake struct{}
type IDUUID struct{}
type IDUUIDv7 struct{}
type IDUUIDv4 struct{}
type IDULID struct{}
type IDKSUID struct{}

func (IDAuto) Strategy() Strategy       { return StrategyDBAuto }
func (IDAuto) UUIDVersion() string      { return "" }
func (IDSnowflake) Strategy() Strategy  { return StrategySnowflake }
func (IDSnowflake) UUIDVersion() string { return "" }
func (IDUUID) Strategy() Strategy       { return StrategyUUID }
func (IDUUID) UUIDVersion() string      { return "" }
func (IDUUIDv7) Strategy() Strategy     { return StrategyUUID }
func (IDUUIDv7) UUIDVersion() string    { return "v7" }
func (IDUUIDv4) Strategy() Strategy     { return StrategyUUID }
func (IDUUIDv4) UUIDVersion() string    { return "v4" }
func (IDULID) Strategy() Strategy       { return StrategyULID }
func (IDULID) UUIDVersion() string      { return "" }
func (IDKSUID) Strategy() Strategy      { return StrategyKSUID }
func (IDKSUID) UUIDVersion() string     { return "" }
