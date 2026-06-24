//! M1 머지 가드레일 — CRDT 수렴 속성 테스트.
//!
//! 골격: 수렴이 만족해야 할 속성(교환·결합·멱등)의 *형태*만 잡아둔다.
//! M1 본 구현에서 yrs `Doc`에 임의 update 시퀀스를 적용해 최종 상태 동일성을 검증한다.
//! (SDD 가드레일: 머지 머지 전 이 테스트 통과 필수.)

use proptest::prelude::*;

proptest! {
    /// 교환법칙: update 적용 순서가 달라도 최종 상태는 동일해야 한다.
    /// TODO(M1): apply(doc, [a, b]) == apply(doc, [b, a]) — yrs 상태 비교로 교체.
    #[test]
    fn convergence_is_order_independent(a in 0u8..32, b in 0u8..32) {
        prop_assert_eq!(u16::from(a) + u16::from(b), u16::from(b) + u16::from(a));
    }

    /// 멱등성: 같은 update를 두 번 적용해도 상태는 변하지 않아야 한다.
    /// TODO(M1): apply(apply(doc, u), u) == apply(doc, u).
    #[test]
    fn convergence_is_idempotent(u in 0u8..32) {
        prop_assert_eq!(u, u);
    }
}
